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
- Keep reusable HTTPS transport under [jenkins/databricks](../../../jenkins/databricks/);
  GitHub selection, attempt reporting and orphan cleanup belong here.
  The existing build and test scripts select and cache the Maven and Spark
  distributions in `/dbfs/cached_jars`. Do not duplicate their version list.
- The protected [cache job](../cache.yml) seeds only released third-party POMs
  and JARs from Central. Drivers verify the digest from trusted main, not DBFS,
  before copying them into a private Maven repository. Do not seed local
  installs, repository credentials, JNI/private artifacts or SNAPSHOTs.
  A manual cache refresh replaces even a same-size damaged workspace seed.
- Run `python3 -m unittest discover -s .github/workflows/databricks-ci` and
  `python3 -m unittest discover -s jenkins/databricks/tests` from the repo root.
