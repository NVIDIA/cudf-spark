# Project Aether Agent Skills

Aether Agent is a set of skills to convert Apache Spark User-Defined Functions (UDFs) for GPU acceleration with the [RAPIDS Accelerator for Apache Spark](https://github.com/NVIDIA/spark-rapids). It provides:

1. **Test generation** -- Create unit tests and test data for existing UDFs.
2. **Conversion** -- Convert a UDF to a GPU-compatible implementation (SQL, cuDF RapidsUDF, or native CUDA RapidsUDF).
3. **Benchmarking** -- Generate synthetic data and benchmark the original UDF against the GPU implementation.
4. **Optimization** -- Iteratively profile and optimize a cuDF RapidsUDF for GPU performance.

<details open>
<summary><strong>Table of Contents</strong></summary>

- [Installation](#installation)
- [Supported Formats](#supported-formats)
- [Prerequisites](#prerequisites)
- [Selecting an LLM](#selecting-an-llm)
- [Quick Start](#quick-start)
  - [Using Skills](#using-skills)
  - [Try the Workflow](#try-the-workflow)

</details>

## Installation

Install via the [skills CLI](https://github.com/vercel-labs/skills). Installing all skills is recommended, as they are designed to work together.

```bash
npx skills add NVIDIA/spark-rapids --skill '*' [--agent <agent>]
```

## Supported Formats

| UDF Type  | cuDF RapidsUDF | CUDA RapidsUDF | Spark SQL |
|-----------|----------------|------------------------|-----------|
| Java UDF  | Yes | Yes | Yes |
| Hive UDF  | Yes | Yes | Yes |
| Scala UDF | Yes | Yes | Yes |
| Java UDAF | -- | -- | Yes |
| Hive UDAF | -- | -- | Yes |
| Scala UDAF | -- | -- | Yes |

## Prerequisites

- **[Maven](https://maven.apache.org/install.html)** is required to build/compile UDFs.
- **[JDK](https://docs.oracle.com/en/java/javase/index.html)** must be installed on the system.
- **Local GPU** with [CUDA toolkit](https://developer.nvidia.com/cuda/toolkit) is required (see [Spark RAPIDS compatibility](https://nvidia.github.io/cudf-spark/docs/download.html) for version requirements).

If a local GPU is not available, another option is to run Aether Agent from a cloud instance, such as AWS EC2.

## Selecting an LLM

For best results, we recommend the latest reasoning models from OpenAI, Anthropic, or Google. As a good proxy, models near the top of the [Terminal-Bench 2.0 leaderboard](https://www.tbench.ai/leaderboard/terminal-bench/2.0) tend to perform well.

## Quick Start

Skills require any IDE or LLM that supports the [agent skills spec](https://agentskills.io) (e.g., Cursor, Codex, Claude Code).

### Using Skills

Skills follow a multi-step workflow:

1. **[udf-gen-test](udf-gen-test/SKILL.md)** -- Generate a unit test for the UDF
2. **[udf-convert-to-cudf](udf-convert-to-cudf/SKILL.md)**, **[udf-convert-to-cuda](udf-convert-to-cuda/SKILL.md)**, or **[udf-convert-to-sql](udf-convert-to-sql/SKILL.md)** -- Convert the UDF to a GPU-compatible implementation
3. **[udf-judge-conversion](udf-judge-conversion/SKILL.md)** -- Review generated tests and implementations for coverage gaps, bugs, and edge cases
4. **[udf-benchmark](udf-benchmark/SKILL.md)** -- Benchmark CPU vs GPU performance
5. **[udf-optimize-cudf](udf-optimize-cudf/SKILL.md)** -- Iteratively profile and optimize the cuDF RapidsUDF

To invoke a skill, use your IDE's skill command, or simply describe the task and let the agent load the skill automatically.

```bash
# Manual invocation
❯ Use the /udf-gen-test skill to generate a unit test for @FormatPhoneUDF.java

# Automatic invocation
❯ Generate a unit test for @FormatPhoneUDF.java
```

Each skill builds on the output of the previous one -- udf-gen-test produces a project with a passing unit test, which the conversion skills use as input, and the udf-benchmark skill uses the conversion output.

You can invoke multiple steps in a single prompt:

```bash
❯ Generate a unit test for @FormatPhoneUDF.java, then convert it to cuDF, native CUDA, or SQL and benchmark
```

### Try the Workflow

Once you've installed the skills, try the workflow with one of the provided example UDFs:
- Java: [FormatPhoneUDF.java](examples/FormatPhoneUDF.java)
- Hive: [IntegerMultiplyBy2UDF.java](examples/IntegerMultiplyBy2UDF.java)
- Scala: [CalculateRiskUDF.scala](examples/CalculateRiskUDF.scala)

#### Workflow

(Example dialogue is from Claude Code).

1. Open your project in your IDE of choice with the skills installed.
2. Ask the agent to generate a unit test:

    ```bash
    ❯ Please generate a unit test for @FormatPhoneUDF.java
      ⎿  Read FormatPhoneUDF.java (23 lines)

    ● Skill(udf-gen-test)
      ⎿  Successfully loaded skill
    ● I'll generate a unit test for FormatPhoneUDF.java. Let me start by setting up the project.
    ● ...
    ● Unit test generation is complete. Here's a summary:

      Results

      - Project directory: FormatPhoneUDF/
      - Unit test: FormatPhoneUDF/src/test/scala/com/udf/UnitTest.scala
      - Status: All tests passing
      - Test coverage: 12 cases
   ```

3. Once the test passes, convert the UDF:

    ```bash
    ❯ Please convert the UDF to cuDF

    ● I'll use the udf-convert-to-cudf skill to convert this UDF to a GPU-accelerated RapidsUDF.
    ● Skill(udf-convert-to-cudf)
      ⎿  Successfully loaded skill
    ● ...
    ● Results

      The RapidsUDF conversion is complete.

      - RapidsUDF: FormatPhoneUDF/src/main/java/com/udf/FormatPhoneRapidsUDF.java
      - Comparison test: passes with no memory leaks
    ```

   Alternatively, ask for a judge pass as part of the conversion request:

    ```bash
    ❯ Please convert the UDF to cuDF and run the judge review agent on the result
    ```

   **Note:** The conversion agent will only invoke the judge when you explicitly ask for it. Otherwise, the agent performs a local self-review. You can also invoke the udf-judge-conversion skill on its own, at any time.

4. Finally, benchmark the result:

    ```bash
    ❯ Please benchmark the implementations on 100M rows.

    ● Skill(udf-benchmark)
      ⎿  Successfully loaded skill
    ● ...
    ● Benchmark Results — 100M rows

      ┌─────────┬─────────────┐
      │  Mode   │ Runtime (s) │
      ├─────────┼─────────────┤
      │ CPU     │ 16.27       │
      ├─────────┼─────────────┤
      │ GPU     │ 6.52        │
      ├─────────┼─────────────┤
      │ Speedup │ 2.50x       │
      └─────────┴─────────────┘

      The GPU RapidsUDF implementation is 2.5x faster than the CPU UDF on 100 million rows.
    ```

5. Optionally for cuDF RapidsUDF conversions, optimize the implementation:

    ```bash
    ❯ Please optimize the implementation

    ● Skill(udf-optimize-cudf)
      ⎿  Successfully loaded skill
    ● ...
    ```
