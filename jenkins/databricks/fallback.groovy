/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Opt-in CI orchestration for the repository's existing Databricks build/test scripts.
Map run(script, dbUtils, Closure build, Closure test) {
    def preferred = script.env.DB_TYPE
    if (!(preferred in ['aws', 'azure3'])) {
        script.error 'Databricks fallback supports only aws/azure3 profiles'
    }
    def alternate = preferred == 'aws' ? 'azure3' : 'aws'
    return run(script, dbUtils, [preferred, alternate], build, test)
}

Map run(script, dbUtils, List<String> providers, int attemptsPerProvider = 2,
        Closure build, Closure test) {
    if (!providers || providers.any { !dbUtils.getCloud(it) }) {
        script.error 'Databricks fallback requires valid provider profiles'
    }
    if (!(script.env.DB_RUNTIME ==~ /[A-Za-z0-9][A-Za-z0-9_.-]*/) ||
        !(script.env.BUILD_NUMBER ==~ /[0-9]+/) || !script.env.BUILD_TAG ||
        !script.env.WORKSPACE || !script.env.NODE_NAME) {
        script.error 'Databricks fallback requires DB_RUNTIME, BUILD_NUMBER, BUILD_TAG, WORKSPACE and NODE_NAME'
    }
    // Keep per-attempt configuration outside the source tar sent to the cluster.
    script.sh 'mkdir -p .databricks-fallback'
    // Retry orchestration is shared with future CSPUTIL callers; this callback owns cuDF-Spark.
    // Rebuild on each cluster. Publish artifacts/trigger downstream jobs only after run returns.
    return script.databricksRetry(script, providers, attemptsPerProvider) { target, attempt, finalAttempt ->
        def stateFile = "databricks-attempt-${script.env.BUILD_NUMBER}-${script.env.DB_RUNTIME}-${attempt}-${target}.json"
        def initDir = "${script.env.INIT_SCRIPTS_DIR}-${attempt}-${target}"
        def failureFile = ".databricks-fallback/failure-${stateFile}.txt"
        def retryable = false
        def reason = ''
        def operation = 'preparation'
        def testFailed = false
        def classificationAvailable = false
        def gpuDiagnostics = ''
        script.withCredentials([
            script.string(credentialsId: dbUtils.getToken(target), variable: 'DATABRICKS_TOKEN'),
            script.file(credentialsId: 'SPARK_DATABRICKS_PRIVKEY', variable: 'DATABRICKS_PRIVKEY')
        ]) {
            def sshConfig = dbUtils.getSshConfig(target, script.env.DATABRICKS_PRIVKEY)
            def sshConfigFile = sshConfig ?
                "${script.env.WORKSPACE}/.databricks-fallback/ssh-${stateFile}.config" : ''
            script.withEnv([
                "DB_TYPE=$target",
                "DATABRICKS_CLOUD=${dbUtils.getCloud(target)}",
                "DATABRICKS_HOST=${dbUtils.getHost(target)}",
                "DATABRICKS_DRIVER=${dbUtils.getDriver(target)}",
                "DATABRICKS_WORKER=${dbUtils.getWorker(target)}",
                "DATABRICKS_SSH_CONFIG=$sshConfigFile",
                "CLUSTER_NAME=CI-${script.env.BUILD_TAG}-${script.env.DB_RUNTIME}-${attempt}-${target}",
                "INIT_SCRIPTS_DIR=$initDir",
                "DATABRICKS_TEST_FAILURE_FILE=$failureFile",
                "DATABRICKS_READINESS_OBSERVATIONS_FILE=${failureFile}.readiness.json",
                // An Azure-preferred job must not inherit an AWS-specific zone override.
                'DB_ZONE=auto'
            ]) {
                def primaryError = null
                try {
                    if (sshConfigFile) {
                        script.writeFile(file: sshConfigFile, text: sshConfig)
                        script.sh 'chmod 600 "$DATABRICKS_SSH_CONFIG"'
                    }
                    def paths = script.env.INIT_SCRIPTS ?
                        dbUtils.uploadFiles(script, script.env.INIT_SCRIPTS, initDir) : ''
                    script.withEnv(["INIT_SCRIPT_PATHS=$paths"]) {
                        script.stage("Prepare ${script.env.DB_RUNTIME} DB ($target, attempt ${attempt + 1})") {
                            def configFile = ".databricks-fallback/config-${stateFile}"
                            script.writeJSON(file: configFile, json: script.readJSON(returnPojo: true,
                                text: script.sh(returnStdout: true,
                                    script: 'python3 jenkins/databricks/fallback-config.py')))
                            def status = script.sh(returnStatus: true,
                                script: "python3 jenkins/databricks/cluster_fallback.py prepare --state $stateFile --config $configFile")
                            def state = script.fileExists(stateFile) ?
                                script.readJSON(file: stateFile, returnPojo: true) : [:]
                            if (state.schema != 1 || state.workspace != script.env.DATABRICKS_HOST ||
                                state.jenkins_workspace != script.env.WORKSPACE ||
                                state.jenkins_node != script.env.NODE_NAME ||
                                state.build_tag != script.env.BUILD_TAG || state.runtime != script.env.DB_RUNTIME ||
                                state.profile != target) {
                                script.error 'Missing, incompatible or foreign Databricks attempt state'
                            }
                            reason = state.reason ?: 'UNKNOWN'
                            retryable = status == 75 && state.retryable == true
                            script.echo "Databricks preparation: exit=$status, reason=$reason, retryable=$retryable"
                            if (status == 0 && state.phase == 'READY' && state.cluster_id) {
                                script.echo "Databricks ${script.env.DB_RUNTIME}: using $target (attempt ${attempt + 1})"
                                // Compilation failures must never enter the startup classifier.
                                operation = 'build'
                                script.echo 'Databricks: starting build callback (build failures do not trigger fallback)'
                                build(state.cluster_id as String)
                                operation = 'readiness hook installation'
                                script.echo 'Databricks: build callback completed; installing Spark readiness hook'
                                script.sh "python3 jenkins/databricks/cluster_fallback.py install-readiness --state $stateFile"
                                def extraEnvs = script.env.EXTRA_ENVS ? "${script.env.EXTRA_ENVS}," : ''
                                script.withEnv([
                                    "EXTRA_ENVS=${extraEnvs}SPARK_STARTUP_CHECK=/home/ubuntu/.databricks-fallback/spark_readiness.sh",
                                    // Opt in only here; manual run-tests.py callers keep their defaults.
                                    'DATABRICKS_CI_TEST_SUPERVISION=1'
                                ]) {
                                    try {
                                        operation = 'test'
                                        script.echo 'Databricks: starting test callback with Spark readiness checks'
                                        test(state.cluster_id as String)
                                        script.echo 'Databricks: test callback completed; verifying readiness results'
                                    } catch (hudson.AbortException testError) {
                                        testFailed = true
                                        gpuDiagnostics = collectGpuDiagnostics(script, stateFile)
                                        // Interruptions bypass this catch. Product failures have no failed readiness marker.
                                        script.echo 'Databricks: test callback failed; checking for a classified startup failure'
                                        try {
                                            def classification = script.sh(returnStatus: true,
                                                script: "python3 jenkins/databricks/cluster_fallback.py classify-startup --state $stateFile")
                                            state = script.readJSON(file: stateFile, returnPojo: true)
                                            classificationAvailable = state.classification_available == true
                                            retryable = classificationAvailable && classification == 75 && state.retryable == true
                                            reason = state.reason ?: 'UNKNOWN'
                                            script.echo "Databricks startup classification: exit=$classification, reason=$reason, retryable=$retryable"
                                            if (!classificationAvailable) {
                                                script.echo 'Startup classification unavailable; preserving the original failure'
                                            }
                                        } catch (org.jenkinsci.plugins.workflow.steps.FlowInterruptedException interrupted) {
                                            throw interrupted
                                        } catch (Exception diagnosticError) {
                                            script.echo 'Startup classification unavailable; preserving the original failure'
                                            throw testError
                                        }
                                        if (!retryable || finalAttempt) {
                                            script.echo 'Databricks: no provider retry; propagating the test failure'
                                            throw testError
                                        }
                                    }
                                    if (!retryable) {
                                        operation = 'readiness verification'
                                        script.sh "python3 jenkins/databricks/cluster_fallback.py verify-readiness --state $stateFile"
                                    }
                                }
                            } else if (!retryable || finalAttempt) {
                                script.error "Databricks preparation failed on $target: $reason"
                            }
                        }
                    }
                } catch (Exception error) {
                    primaryError = error
                    throw error
                } finally {
                    def cleanupFailed = false
                    try {
                        cleanupAttempt(script, dbUtils, stateFile, initDir, primaryError)
                    } catch (Exception cleanupError) {
                        cleanupFailed = true
                        throw cleanupError
                    } finally {
                        if (sshConfigFile) {
                            script.sh(returnStatus: true, script: 'rm -f -- "$DATABRICKS_SSH_CONFIG"')
                        }
                        if (primaryError || retryable || cleanupFailed) {
                            printFailureSummary(script, stateFile, failureFile,
                                cleanupFailed && !primaryError && !retryable ? 'cleanup' : operation,
                                testFailed, classificationAvailable, retryable, finalAttempt, reason,
                                gpuDiagnostics)
                        }
                    }
                }
            }
        }
        // Cleanup failures throw above; the coordinator must never allocate another cluster then.
        def cleanedState = script.readJSON(file: stateFile, returnPojo: true)
        return [retryable: retryable, reason: reason, cleanup_complete: cleanedState.cleanup_complete == true]

    }
}

String collectGpuDiagnostics(script, String stateFile) {
    try {
        // Capture before cleanup, but leave the test log focused on its original Spark failure.
        return script.sh(returnStdout: true, label: 'Collect Databricks GPU diagnostics',
            script: "set +x\npython3 jenkins/databricks/cluster_fallback.py gpu-diagnostics --state $stateFile 2>&1 || :").trim()
    } catch (org.jenkinsci.plugins.workflow.steps.FlowInterruptedException interrupted) {
        throw interrupted
    } catch (Exception ignored) {
        return 'GPU diagnostics unavailable; preserving the original test failure.'
    }
}

void printFailureSummary(script, String stateFile, String failureFile, String operation,
                         boolean testFailed, boolean classificationAvailable,
                         boolean retryable, boolean finalAttempt, String reason,
                         String gpuDiagnostics = '') {
    try {
        def state = script.fileExists(stateFile) ?
            script.readJSON(file: stateFile, returnPojo: true) : [:]
        def lines = ["=== Databricks failure summary: DBR ${script.env.DB_RUNTIME}, ${script.env.DB_TYPE} ===",
                     "Failed operation: $operation"]
        if (testFailed && script.fileExists(failureFile)) {
            def testSummary = script.readFile(file: failureFile).trim()
            if (testSummary) {
                lines.add(testSummary)
            }
        }
        def readiness = state.spark_readiness ?: [:]
        def observationsFile = "${failureFile}.readiness.json"
        def observations = script.fileExists(observationsFile) ?
            script.readJSON(file: observationsFile, returnPojo: true) : [:]
        (state.readiness_modes ?: []).each { mode ->
            def observed = observations[mode] in ['READY', 'FAILED'] ? observations[mode] : null
            def phase = readiness[mode]?.phase ?: (observed ? "$observed (observed in test output)" : 'not recorded')
            lines.add("Spark readiness ($mode): $phase")
        }
        def classified = operation == 'preparation' || classificationAvailable
        def classificationReason = classified ? reason ?: 'unavailable' : 'not available for this failure'
        lines.add("Retry classification: $classificationReason, retryable=$retryable")
        if (testFailed && !classificationAvailable) {
            lines.add('No retry: startup classification was unavailable; preserving the test failure.')
        } else if (reason == 'NOT_A_CLASSIFIED_SPARK_STARTUP_FAILURE') {
            def modes = state.readiness_modes ?: []
            lines.add(modes && modes.every { readiness[it]?.phase == 'READY' } ?
                'No retry: Spark readiness already passed; later product/test failures do not trigger fallback.' :
                'No retry: no classified preflight failure was recorded.')
        } else if (retryable) {
            lines.add(finalAttempt ? 'No retry: attempt limit reached.' :
                'Retry eligible; the coordinator proceeds only after successful cleanup.')
        } else {
            lines.add('No retry: this failure is not classified as retryable.')
        }
        lines.add("Cluster termination confirmed: ${state.termination_confirmed == true}")
        lines.add("Cluster cleanup complete: ${state.cleanup_complete == true}")
        if (gpuDiagnostics) {
            lines.add('GPU snapshot collected after test failure, before cluster cleanup:')
            lines.add('=== Databricks GPU diagnostics ===')
            lines.add(gpuDiagnostics)
            lines.add('=== End Databricks GPU diagnostics ===')
        }
        lines.add('=== End Databricks failure summary ===')
        script.echo lines.join('\n')
    } catch (Exception summaryError) {
        // Interruptions must propagate even when a test or cleanup failure is already pending.
        if (summaryError instanceof org.jenkinsci.plugins.workflow.steps.FlowInterruptedException) {
            throw summaryError
        }
        // Do not log again here: even a warning can fail and hide the pending exception.
    }
}

void cleanupAttempt(script, dbUtils, String stateFile, String initDir, Exception primaryError) {
    def cleanupError = null
    script.echo "Databricks: cleaning attempt recorded in $stateFile"
    try {
        if (script.fileExists(stateFile)) {
            script.timeout(time: 5, unit: 'MINUTES') {
                script.retry(3) {
                    script.sh "python3 jenkins/databricks/cluster_fallback.py cleanup --state $stateFile"
                }
            }
        }
    } catch (Exception error) {
        cleanupError = error
        script.echo "Failed to terminate Databricks attempt; cluster handle is in $stateFile"
    }
    try {
        if (script.env.INIT_SCRIPTS) {
            script.echo 'Databricks: cleaning attempt init scripts'
            dbUtils.cleanUp(script, initDir)
        }
    } catch (Exception error) {
        if (!cleanupError || error instanceof org.jenkinsci.plugins.workflow.steps.FlowInterruptedException) {
            cleanupError = error
        }
        script.echo 'Failed to clean Databricks init scripts'
    }
    try {
        script.echo 'Databricks: archiving attempt state'
        script.archiveArtifacts(artifacts: "$stateFile,.databricks-fallback/failure-${stateFile}.txt," +
            ".databricks-fallback/failure-${stateFile}.txt.readiness.json", allowEmptyArchive: true)
    } catch (Exception error) {
        if (!cleanupError || error instanceof org.jenkinsci.plugins.workflow.steps.FlowInterruptedException) {
            cleanupError = error
        }
        script.echo 'Failed to archive Databricks diagnostics'
    }
    if (cleanupError && !primaryError) {
        throw cleanupError
    }
    script.echo(cleanupError ? 'Databricks: cleanup incomplete; preserving the primary failure' :
        'Databricks: attempt cleanup completed')
}

return this
