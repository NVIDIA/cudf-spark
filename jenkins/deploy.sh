#!/bin/bash
#
# Copyright (c) 2020-2026, NVIDIA CORPORATION. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Argument(s):
#   SIGN_FILE:  true/false, whether to sign the jar/pom file to de deployed
#
# Used environment(s):
#   SQL_PL:         The path of module 'sql-plugin', relative to project root path.
#   DIST_PL:        The path of module 'dist', relative to project root path.
#   SERVER_ID:      The repository id for this deployment.
#   SERVER_URL:     The url where to deploy artifacts.
#   GPG_PASSPHRASE: The passphrase used to sign files, only required when <SIGN_FILE> is true.
#   SIGN_TOOL:      Tool to sign files, e.g., gpg, nvsec, only required when $1 is 'true'
#   GPG_PASSPHRASE: gpg passphrase to sign artifacts, only required when <SIGN_TOOL> is gpg
#   MVN_SETTINGS:   Maven configuration file
#   PARENT_POM:     Comma-separated parent pom files to be deployed
#   POM_FILE:       Project pom file to be deployed
#   OUT_PATH:       The path where jar files are
#   CUDA_CLASSIFIERS:    Comma separated classifiers, e.g., "cuda12"
#   CLASSIFIERS:    Comma separated classifiers, e.g., "cuda12,cuda12-arm64"
#   DEFAULT_CUDA_CLASSIFIER: The default cuda classifer, will get from project's pom.xml if not set
###

set -ex

SIGN_FILE=${1:-"false"}
DIST_PL=${DIST_PL:-"dist"}

###### Build the path of jar(s) to be deployed ######
MVN_SETTINGS=${MVN_SETTINGS:-"jenkins/settings.xml"}
MVN="mvn -B -Dmaven.wagon.http.retryHandler.count=3 -DretryFailedDeploymentCount=3 -s $MVN_SETTINGS"
SCALA_BINARY_VER=${SCALA_BINARY_VER:-"2.12"}
[ $SCALA_BINARY_VER == "2.13" ] && MVN="$MVN -f scala2.13/"

function mvnEval {
    $MVN help:evaluate -q -DforceStdout -pl $1 -Dexpression=$2
}
ART_ID=$(mvnEval $DIST_PL project.artifactId)
ART_GROUP_ID=$(mvnEval $DIST_PL project.groupId)
ART_VER=$(mvnEval $DIST_PL project.version)
DEFAULT_CUDA_CLASSIFIER=${DEFAULT_CUDA_CLASSIFIER:-$(mvnEval $DIST_PL cuda.version)}
CUDA_CLASSIFIERS=${CUDA_CLASSIFIERS:-"$DEFAULT_CUDA_CLASSIFIER"}
CLASSIFIERS=${CLASSIFIERS:-"$CUDA_CLASSIFIERS"} # default as CUDA_CLASSIFIERS for compatibility
SERVER_ID=${SERVER_ID:-"snapshots"}
SERVER_URL=${SERVER_URL:-"file:/tmp/local-release-repo"}
# Save to be deployed artifact list into the file, e.g.
ARTIFACT_FILE=${ARTIFACT_FILE:-"/tmp/artifact-file"}
# Clean rtifact list file befor saving
rm -rf $ARTIFACT_FILE

SQL_PL=${SQL_PL:-"sql-plugin"}
POM_FILE=${POM_FILE:-"$DIST_PL/target/parallel-world/META-INF/maven/${ART_GROUP_ID}/${ART_ID}/pom.xml"}
OUT_PATH=${OUT_PATH:-"$DIST_PL/target"}
SIGN_TOOL=${SIGN_TOOL:-"gpg"}

FPATH="$OUT_PATH/$ART_ID-$ART_VER"
DEPLOY_TYPES=''
DEPLOY_FILES=''
DEPLOY_TYPES=$(echo $CLASSIFIERS | sed -e 's;[^,]*;jar;g')
DEPLOY_FILES=$(echo $CLASSIFIERS | sed -e "s;\([^,]*\);${FPATH}-\1.jar;g")

# dist does not have javadoc and sources jars, use 'sql-plugin' instead
SQL_ART_ID=$(mvnEval $SQL_PL project.artifactId)
SQL_ART_VER=$(mvnEval $SQL_PL project.version)
JS_FPATH="$(echo -n ${SQL_PL}/target/spark*)/${SQL_ART_ID}-${SQL_ART_VER}"
if [ $SCALA_BINARY_VER == "2.13" ]; then
    cp scala2.13/$JS_FPATH-sources.jar scala2.13/$FPATH-sources.jar
    cp scala2.13/$JS_FPATH-javadoc.jar scala2.13/$FPATH-javadoc.jar
else
    cp $JS_FPATH-sources.jar $FPATH-sources.jar
    cp $JS_FPATH-javadoc.jar $FPATH-javadoc.jar
fi
echo "Plan to deploy ${FPATH}.jar to $SERVER_URL (ID:$SERVER_ID)"

GPG_PLUGIN="org.apache.maven.plugins:maven-gpg-plugin:3.1.0:sign-and-deploy-file"
###### Choose the deploy command ######
if [ "$SIGN_FILE" == true ]; then
    case $SIGN_TOOL in
        nvsec)
            DEPLOY_CMD="$MVN $GPG_PLUGIN -Dgpg.executable=nvsec_sign"
            ;;
        gpg)
            DEPLOY_CMD="$MVN $GPG_PLUGIN -Dgpg.passphrase=$GPG_PASSPHRASE "
            ;;
        *)
            echo "Error unsupported sign type : $SIGN_TYPE !"
            echo "Please set variable SIGN_TOOL 'nvsec'or 'gpg'"
            exit 255
            ;;
    esac
else
    DEPLOY_CMD="$MVN -B deploy:deploy-file"
fi
DEPLOY_CMD="$DEPLOY_CMD -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID"
echo "Deploy CMD: $DEPLOY_CMD"

###### Deploy the parent pom file(s) ######
PARENT_POM=${PARENT_POM:-"./pom.xml"}
IFS=',' read -ra parent_arr <<< "$PARENT_POM"
for parent in "${parent_arr[@]}"; do
    $DEPLOY_CMD -Dfile="$parent" -DpomFile="$parent"
done

###### Deploy the artifact jar(s) ######
$DEPLOY_CMD -DpomFile=$POM_FILE \
            -Dfile=$FPATH-$DEFAULT_CUDA_CLASSIFIER.jar \
            -Dsources=$FPATH-sources.jar \
            -Djavadoc=$FPATH-javadoc.jar \
            -Dfiles=$DEPLOY_FILES \
            -Dtypes=$DEPLOY_TYPES \
            -Dclassifiers=$CLASSIFIERS

echo "$ART_GROUP_ID:$ART_ID:$ART_VER:jar" >> $ARTIFACT_FILE
CLASSLIST="$CLASSIFIERS,sources,javadoc"
IFS=", " read -ra CLASSLIST <<< "$CLASSLIST"
for class in "${CLASSLIST[@]}"; do
    echo "$ART_GROUP_ID:$ART_ID:$ART_VER:jar:$class" >> $ARTIFACT_FILE
done
