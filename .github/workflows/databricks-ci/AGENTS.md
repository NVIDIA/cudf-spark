# Maintaining Databricks CI

- [paths.json](paths.json) separates CPU helper-test triggers from cluster
  selection. Reserve `cluster.exclusive_files` for DB-only scripts: editing one
  selects Databricks without requiring standalone tests. Keep credentials in the trusted controller;
  do not execute PR code on the coordinator.
- [runtimes.py](runtimes.py) reads the Scala 2.13 release profiles and the
  corresponding `DatabricksShimVersion` declarations. Use that result for
  scheduling and reporting. For a new shim, also verify the driver
  [build script](../../../jenkins/databricks/build.sh) selects its Maven profile;
  discovery does not add product support.
- [attempts.py](attempts.py) owns retry decisions and immediate cleanup.
  If job names or ownership tags change, update [prior-cleanup.py](prior-cleanup.py)
  and [cleanup-orphans.py](cleanup-orphans.py) together. Never delete an
  unverified cluster or start a replacement after uncertain teardown.
- [archives.json](archives.json) pins the Maven distribution and the upstream
  Spark smoke archive used by the active DBR rows. Do not store `~/.m2`,
  JNI SNAPSHOTs or private artifacts in DBFS. Check the driver
  [test script](../../../jenkins/databricks/test.sh) for its Scala suffix when
  updating the catalog. Use Apache's SHA-512 and the compressed byte size.
- The protected main [cache job](../cache.yml) checks each workspace on the
  daily schedule or manual dispatch. It checks `dbfs:/cached_jars/cudf-spark-ci/<sha512>/<filename>`
  first. On a miss, one more stat can identify a matching-size legacy candidate at
  `dbfs:/cached_jars/<filename>` and finish the check without transferring the archive.
  The driver verifies SHA-512 before extraction and downloads the pinned distribution
  if the candidate is corrupt. When both paths miss, the warmer verifies the public
  download before publishing to the digest path and reuses those bytes across workspace misses.
  To inspect a single approved workspace, pass its protected `DATABRICKS_HOST`
  and `DATABRICKS_TOKEN` to `prepare_archives.py plan`. Use `--archive NAME`
  to select a single catalog entry. If a driver rejects a legacy archive, run
  `prepare_archives.py ensure --archive NAME --ignore-legacy` with the warmer identity.
  It never overwrites the legacy file. These operations require DBFS root access.
- Run `python3 -m unittest discover -s .github/workflows/databricks-ci` and
  `python3 -m unittest discover -s jenkins/databricks/tests` from the repo root.
