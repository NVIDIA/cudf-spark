# Standalone Spark test runtimes

When a premerge test starts using, changes, or drops a Spark binary distribution,
update the [archive manifest](premerge-spark-cache/releases.json) in the same
change. Use Apache's published SHA-512 for the exact tarball. Keep an archive
until its last test consumer is removed. A shim compiled only through Maven
does not need a Spark tarball.
