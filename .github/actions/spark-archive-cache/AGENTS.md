# Maintaining standalone Spark archives

When a standalone test adds, changes or drops a Spark distribution, update
[releases.json](releases.json) with the exact Apache tarball's published SHA-512.
Keep it until its last standalone consumer is removed. A Maven-only shim does
not need a tarball; Databricks smoke tests use their workspace cache instead.

Run `python3 -m unittest discover -s .github/actions/spark-archive-cache`.
