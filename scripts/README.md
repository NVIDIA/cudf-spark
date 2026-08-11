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
