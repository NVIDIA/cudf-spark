# API validation script for Rapids Plugin

API validation script checks the compatibility of community Spark Execs and GPU Execs in the Rapids Plugin for Spark.  
For example: HashAggregateExec with GpuHashAggregateExec.
Script can be used to audit different versions of Spark.
The script prints Execs where validation fails. 
Validation fails when:
1) The number of parameters differ between community Spark Execs and Gpu Execs.
2) Parameters to the exec have a type mismatch.
3) Order of parameters differ between Spark Exec abd Gpu Exec.

# Dependencies

It requires spark-rapids-jni, rapids-4-spark and Spark jars.

# Running the script

```
cd api_validation
// To run validation script on all version of Spark
sh auditAllVersions.sh

// To run script on particular version we can use profile
mvn scala:run -P spark320
```

# Output
Output is saved in `api-validation-result.log` in api_validation/target directory.  

Sample Output

```
************************************************************************************************
Types differ for below parameters in this Exec
SparkExec  - [org.apache.spark.sql.execution.aggregate.HashAggregateExec]
GpuExec - [com.nvidia.spark.rapids.GpuHashAggregateExec]
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Seq[org.apache.spark.sql.catalyst.expressions.Attribute]          | Seq[com.nvidia.spark.rapids.GpuAttributeReference]

************************************************************************************************
Types differ for below parameters in this Exec
SparkExec  - [org.apache.spark.sql.execution.GenerateExec]
GpuExec - [com.nvidia.spark.rapids.GpuGenerateExec]
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
org.apache.spark.sql.catalyst.expressions.Generator               | Boolean
Seq[org.apache.spark.sql.catalyst.expressions.Attribute]          | Seq[com.nvidia.spark.rapids.GpuExpression]
Boolean                                                           | Seq[org.apache.spark.sql.catalyst.expressions.Attribute]

************************************************************************************************
Types differ for below parameters in this Exec
SparkExec  - [org.apache.spark.sql.execution.ExpandExec]
GpuExec - [com.nvidia.spark.rapids.GpuExpandExec]
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Seq[org.apache.spark.sql.catalyst.expressions.Attribute]          | Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression]

************************************************************************************************
Types differ for below parameters in this Exec
SparkExec  - [org.apache.spark.sql.execution.joins.SortMergeJoinExec]
GpuExec - [com.nvidia.spark.rapids.GpuShuffledHashJoinExec]
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Option[org.apache.spark.sql.catalyst.expressions.Expression]      | org.apache.spark.sql.execution.joins.BuildSide
org.apache.spark.sql.execution.SparkPlan                          | Option[com.nvidia.spark.rapids.GpuExpression]
Boolean                                                           | org.apache.spark.sql.execution.SparkPlan



************************************************************************************************
Parameter lengths don't match between Execs
SparkExec - [org.apache.spark.sql.execution.CollectLimitExec]
GpuExec - [com.nvidia.spark.rapids.GpuCollectLimitExec]
Spark code has 2 parameters where as plugin code has 3 parameters
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Int                                                               | Int
org.apache.spark.sql.execution.SparkPlan                          | com.nvidia.spark.rapids.GpuPartitioning
                                                                  | org.apache.spark.sql.execution.SparkPlan

************************************************************************************************
Parameter lengths don't match between Execs
SparkExec - [org.apache.spark.sql.execution.SortExec]
GpuExec - [com.nvidia.spark.rapids.GpuSortExec]
Spark code has 4 parameters where as plugin code has 5 parameters
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Seq[org.apache.spark.sql.catalyst.expressions.SortOrder]          | Seq[com.nvidia.spark.rapids.GpuSortOrder]
Boolean                                                           | Boolean
org.apache.spark.sql.execution.SparkPlan                          | org.apache.spark.sql.execution.SparkPlan
Int                                                               | com.nvidia.spark.rapids.CoalesceGoal
                                                                  | Int

************************************************************************************************
Parameter lengths don't match between Execs
SparkExec - [org.apache.spark.sql.execution.window.WindowExec]
GpuExec - [com.nvidia.spark.rapids.GpuWindowExec]
Spark code has 4 parameters where as plugin code has 2 parameters
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression]    | Seq[com.nvidia.spark.rapids.GpuExpression]
Seq[org.apache.spark.sql.catalyst.expressions.Expression]         | org.apache.spark.sql.execution.SparkPlan
Seq[org.apache.spark.sql.catalyst.expressions.SortOrder]          | 
org.apache.spark.sql.execution.SparkPlan 
```
