# Auditing Apache Spark's commits 
It can be useful to know how a commit or a set of commits in Apache Spark affect the Plugin. The following 
steps can be helpful in narrowing down which files in the changeset are directly referenced in the Plugin

1. git clone Apache Spark into a folder locally. If you already have the source code, update it and 
   checkout `origin/master`
2. Create a file with a list of commits that you want more information on. The file should include 
   the commit's SHA-1 and a description of the merged PR (The description isn't used but needed for 
   the script to work as expected). 
    ``` 
   ccbd9a7b98  [SPARK-41778][SQL] Add an alias "reduce" to ArrayAggregate
    838954e508  [SPARK-41554] fix changing of Decimal scale when scale decreased by m…
    a77ae27f15  [SPARK-41442][SQL][FOLLOWUP] SQLMetric should not expose -1 value as it's invalid
   ```
3. Run the following command from spark-rapids project-root, and you should get a file called 
  `audit-plugin.log` at location pointed by `$WORKSPACE`. The environment variables must be absolute paths.
   ```
   WORKSPACE=~/workspace SPARK_TREE=~/workspace/spark COMMIT_DIFF_LOG=~/workspace/commits.log 
   ./scripts/prioritize-commits.sh
   ```
4. The `audit-plugin.log` shows a SHA-1 value followed by a list of classes which are changed in this 
commit and are referenced in the Plugin. This should help focus our attention to the relevant changes

## Checking `withResource` nesting

Production Scala code may nest at most four `withResource` scopes. Run the check directly with:

```
python3 scripts/check_with_resource_nesting.py --root .
```

Existing violations are recorded in `scripts/with_resource_nesting_baseline.json`. The baseline is a
ratchet: adding a violation fails the check, and removing one makes the baseline stale. Regenerate it
after deliberately reducing the current violation set. The remaining exceptions are tracked by
https://github.com/NVIDIA/cudf-spark/issues/11713:

```
python3 scripts/check_with_resource_nesting.py --root . --update-baseline
```

When deeper nesting is necessary, place a justified exemption immediately before its outer scope:

```scala
// with-resource-lint: allow-deep-nesting -- required by #11713
withResource(first) { first =>
  // ...
}
```

Only hoist values that own their data. Views returned by methods such as `bitCastTo`,
`getChildColumnView`, `replaceListChild`, and `splitAsViews` must not outlive their owning parent.
Copy or convert a view to an owning resource before closing its parent.

The script remains directly runnable with Python 3, but is also compatible with Jython 2.7. Maven
uses its managed Jython dependency to run the check and unit tests during root `mvn verify`; the
generated Scala 2.13 reactor does not repeat this repository-wide check.

The pull-request audit publishes every deep scope in the job summary and a JSON artifact. Existing
baseline entries are reported as debt but do not fail the check. New violations, stale baseline
entries, invalid exemptions, or report-generation errors fail it. GitHub source annotations show
the first 50 entries, with the complete set retained in the summary and artifact.
