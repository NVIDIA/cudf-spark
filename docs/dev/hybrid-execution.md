---
layout: page
title: The Hybrid(on CPU) execution
nav_order: 14
parent: Developer Overview
---

# The Hybrid(CPU/GPU) execution
Note: this is an experimental feature currently.

## Overview
The Hybrid execution provides a way to offload Parquet scan onto CPU by leveraging Gluten/Velox.

## Configuration
To enable Hybrid Execution, please set the following configurations:
```
"spark.sql.sources.useV1SourceList": "parquet"
"spark.rapids.sql.hybrid.parquet.enabled": "true"
"spark.rapids.sql.hybrid.loadBackend": "true"
```

## Build
### Build Gluten bundle and third party jars.
Hybrid execution targets Gluten v1.2.0 code tag.
For the Gluten building, please refer to [link](https://github.com/apache/incubator-gluten).
Start the docker Gluten project provided, then execute the following
```bash
git clone https://github.com/apache/incubator-gluten.git
git checkout v1.2.0
# Cherry pick a fix from main branch: Fix ObjectStore::stores initialized twice issue
git cherry-pick 2a6a974d6fbaa38869eb9a0b91b2e796a578884c
./dev/package.sh
```
Note: Should cherry-pick a fix as shown in the above steps.
In the $Gluten_ROOT/package/target, you can get the bundle third_party jars.

### Download Rapids Hybrid jar from Maven repo
```xml
<dependency>
   <groupId>com.nvidia</groupId>
   <artifactId>rapids-4-spark-hybrid_${scala.binary.version}</artifactId>
   <version>${project.version}</version>
</dependency>
```

## How to use
Decide the Spark version. Set the configurations as described in the above section.
Prepare the Gluten bundle and third party jars for the Spark version as described
in the above section. Get the Rapids Hybrid jar. Put the jars(Gluten two jars and
the Rapids hybrid jar) in the classpath by specifying:
`--jars=<gluten-bundle-jar>,<gluten-thirdparty-jar>,<rapids-hybrid-jar>`

## Limitations
- Only supports CPU architecture: x86_64
- Only supports OS: Ubuntu 20.04, Ubuntu 22.04
- Only tested Java versions: 8, 11, 17
- Only supports V1 Parquet data source.
- Only supports Scala 2.12, do not support Scala 2.13.
- Support Spark 3.2.2, 3.3.1, 3.4.2, and 3.5.1, matching [Gluten](https://github.com/apache/incubator-gluten/releases/tag/v1.2.0).
Other Spark versions 32x, 33x, 34x, 35x may work, but are not fully tested.
