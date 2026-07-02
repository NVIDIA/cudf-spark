# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Integration tests for the JVM skill templates.
"""

import os
import shutil
import stat
import tempfile
from pathlib import Path

import pytest

from . import fixtures as fx
from .utils import replace_scala_todo_method, run_mvn, run_script

pytestmark = pytest.mark.slow

SKILLS_DIR = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = SKILLS_DIR / "udf-gen-test" / "templates" / "scala"
CUDA_TEMPLATES_DIR = SKILLS_DIR / "udf-convert-to-cuda" / "templates" / "cuda"

LANGS = ["scala", "java"]
TARGETS = ["cudf", "sql", "cuda"]
LANG_TARGETS = [(lang, target) for lang in LANGS for target in TARGETS]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unit_test_methods(lang: str) -> dict[str, str]:
    return {
        "createTestData": fx.CREATE_TEST_DATA,
        "registerUDF": (
            fx.SCALA_REGISTER_UDF if lang == "scala" else fx.JAVA_REGISTER_UDF
        ),
        "executeUDF": fx.EXECUTE_UDF,
        "assertUDFResults": fx.ASSERT_UDF_RESULTS,
    }


def _comparison_test_methods(lang: str, target: str) -> dict[str, str]:
    match (lang, target):
        case ("scala", "cudf"):
            return {"registerRapidsUDF": fx.SCALA_REGISTER_RAPIDS_UDF}
        case ("java", "cudf"):
            return {"registerRapidsUDF": fx.JAVA_REGISTER_RAPIDS_UDF}
        case (_, "cuda"):
            return {"registerRapidsUDF": fx.NATIVE_REGISTER_RAPIDS_UDF}
        case _:  # sql (no additional method)
            return {}


def _bench_utils_methods(lang: str, target: str) -> dict[str, str]:
    methods = {
        "generateSyntheticData": fx.BENCH_GENERATE,
        "executeCpu": (
            fx.BENCH_EXECUTE_SCALA_CPU if lang == "scala" else fx.BENCH_EXECUTE_JAVA_CPU
        ),
    }
    match (lang, target):
        case ("scala", "cudf"):
            methods["executeGpu"] = fx.BENCH_EXECUTE_SCALA_CUDF
        case ("java", "cudf"):
            methods["executeGpu"] = fx.BENCH_EXECUTE_JAVA_CUDF
        case (_, "cuda"):
            methods["executeGpu"] = fx.BENCH_EXECUTE_CUDA
        case _:  # sql
            methods["executeGpu"] = fx.BENCH_EXECUTE_SQL
    return methods


def _micro_bench_methods(lang: str, target: str) -> dict[str, str]:
    return {
        "prepareCpuData": fx.MICRO_PREPARE_CPU,
        "executeCpu": (
            fx.MICRO_EXECUTE_SCALA_CPU if lang == "scala" else fx.MICRO_EXECUTE_JAVA_CPU
        ),
        # microbenchmarks are not applicable to sql
        "executeGpu": (
            fx.MICRO_EXECUTE_CUDA if target == "cuda" else fx.MICRO_EXECUTE_CUDF
        ),
    }


def _build_project_dir() -> tuple[str, str]:
    """Copy export directory to a temp directory and resolve pom.xml."""
    tmp_dir = tempfile.mkdtemp(prefix="test_jvm_")
    project_dir = os.path.join(tmp_dir, "project")
    shutil.copytree(str(TEMPLATES_DIR), project_dir)
    return tmp_dir, project_dir


def _copy_cuda_templates(project_dir: str):
    """Copy CUDA add-on templates and replace placeholders with fixture sources."""
    shutil.copytree(str(CUDA_TEMPLATES_DIR), project_dir, dirs_exist_ok=True)

    extract_script = Path(project_dir) / "native" / "scripts" / "extract-cudf-libs.sh"
    extract_script.chmod(
        extract_script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    )

    project_path = Path(project_dir)
    for rel_path in fx.NATIVE_PLACEHOLDER_FILES:
        (project_path / rel_path).unlink(missing_ok=True)

    for rel_path, source in fx.NATIVE_SOURCE_FILES.items():
        path = project_path / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)

    wrapper = (
        project_path
        / "src"
        / "main"
        / "java"
        / "com"
        / "udf"
        / f"{fx.NATIVE_UDF_NAME}.java"
    )
    wrapper.parent.mkdir(parents=True, exist_ok=True)
    wrapper.write_text(fx.NATIVE_RAPIDS_UDF_SOURCE)

    cmake_path = project_path / "native" / "src" / "main" / "cpp" / "CMakeLists.txt"
    cmake = cmake_path.read_text()
    cmake = cmake.replace(
        """\
set(SOURCE_FILES
  "src/PlaceholderUDFNameJni.cpp"
  "src/placeholder_udf_name.cu"
)
""",
        fx.CMAKE_SOURCE_FILES,
    )
    cmake_path.write_text(cmake)


def _replace_stubs(path: str, methods: dict[str, str]):
    """Replace each named TODO stub in a Scala source file with its implementation."""
    with open(path, "r") as f:
        source = f.read()
    for method_name, impl in methods.items():
        source = replace_scala_todo_method(source, method_name, impl)
    with open(path, "w") as f:
        f.write(source)


def _fill_stubs(project_dir: str, lang: str, target: str):
    """Write UDF sources and fill in all TODO stubs for the (language, target)."""
    if lang == "scala":
        ext = ".scala"
        udf_source = fx.SCALA_UDF_SOURCE
        rapids_udf_source = fx.SCALA_RAPIDS_UDF_SOURCE
    else:
        ext = ".java"
        udf_source = fx.JAVA_UDF_SOURCE
        rapids_udf_source = fx.JAVA_RAPIDS_UDF_SOURCE

    udf_dir = os.path.join(project_dir, "src", "main", lang, "com", "udf")
    bench_dir = os.path.join(project_dir, "src", "main", "scala", "com", "udf", "bench")
    test_dir = os.path.join(project_dir, "src", "test", "scala", "com", "udf")
    os.makedirs(udf_dir, exist_ok=True)

    # Write CPU UDF source.
    with open(os.path.join(udf_dir, f"IntegerMultiplyBy2UDF{ext}"), "w") as f:
        f.write(udf_source)

    # Fill in unit test stubs.
    _replace_stubs(os.path.join(test_dir, "UnitTest.scala"), _unit_test_methods(lang))

    if target == "cudf":
        # Write RapidsUDF
        with open(os.path.join(udf_dir, f"IntegerMultiplyBy2RapidsUDF{ext}"), "w") as f:
            f.write(rapids_udf_source)

        # Fill in comparison test stubs.
        _replace_stubs(
            os.path.join(test_dir, "CudfComparisonTest.scala"),
            _comparison_test_methods(lang, "cudf"),
        )
        # Fill in MicroBenchRunner stubs.
        _replace_stubs(
            os.path.join(bench_dir, "MicroBenchRunner.scala"),
            _micro_bench_methods(lang, target),
        )
    elif target == "cuda":
        # Writes the Java wrapper and C++ sources
        _copy_cuda_templates(project_dir)
        # Fill in comparison test stubs.
        _replace_stubs(
            os.path.join(test_dir, "CudfComparisonTest.scala"),
            _comparison_test_methods(lang, "cuda"),
        )
        # Fill in MicroBenchRunner stubs.
        _replace_stubs(
            os.path.join(bench_dir, "MicroBenchRunner.scala"),
            _micro_bench_methods(lang, target),
        )
    else:  # sql
        # Write SQL file
        resources_dir = os.path.join(project_dir, "src", "main", "resources")
        os.makedirs(resources_dir, exist_ok=True)
        with open(os.path.join(resources_dir, "integer_multiply_by_2.sql"), "w") as f:
            f.write(fx.SQL_SOURCE)

        # Point the comparison test at the SQL file / registered name
        sql_test_path = os.path.join(test_dir, "SqlComparisonTest.scala")
        with open(sql_test_path, "r") as f:
            content = f.read()
        with open(sql_test_path, "w") as f:
            f.write(content.replace("placeholder_udf_name", "integer_multiply_by_2"))

    # Fill in BenchUtils stubs.
    _replace_stubs(
        os.path.join(bench_dir, "BenchUtils.scala"),
        _bench_utils_methods(lang, target),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def project_dir():
    """Clean copy of the export template with resolved pom.xml (stubs not filled)."""
    tmp_dir, proj = _build_project_dir()
    try:
        yield proj
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture(
    scope="module", params=LANG_TARGETS, ids=lambda p: f"{p[0]}-{p[1]}"
)  # 'language-target'
def project_with_fixtures(request):
    """Clean copy with all stubs filled in, per (language, target)."""
    lang, target = request.param
    tmp_dir, proj = _build_project_dir()
    try:
        _fill_stubs(proj, lang, target)
        yield (proj, lang, target)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture(scope="class", params=TARGETS, ids=lambda t: t)
def project_with_broken_gpu(request):
    """Project with deliberately broken GPU implementation."""
    target = request.param
    lang = "scala"
    tmp_dir, proj = _build_project_dir()

    try:
        _fill_stubs(proj, lang, target)

        ext = ".scala" if lang == "scala" else ".java"
        if target == "cudf":
            path = os.path.join(
                proj,
                "src",
                "main",
                lang,
                "com",
                "udf",
                f"IntegerMultiplyBy2RapidsUDF{ext}",
            )
        elif target == "cuda":
            path = os.path.join(
                proj, "native", "src", "main", "cpp", "src", "integer_multiply_by_2.cu"
            )
        else:
            path = os.path.join(
                proj, "src", "main", "resources", "integer_multiply_by_2.sql"
            )

        with open(path, "r") as f:
            content = f.read()

        if target == "cudf":

            def _insert_memory_leak(source: str) -> str:
                # Inject an unclosed Scalar.
                idx = source.index("evaluateColumnar")
                brace = source.index("{", idx)
                return (
                    source[: brace + 1]
                    + '\nScalar.fromString("LEAKED");'
                    + source[brace + 1 :]
                )

            # change multiplier from 2 to 3 and insert memory leak
            broken = content.replace("fromInt(2)", "fromInt(3)")
            broken = _insert_memory_leak(broken)
        else:
            # change multiplier from 2 to 3
            broken = content.replace("* 2", "* 3")

        with open(path, "w") as f:
            f.write(broken)

        yield (proj, target)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture(scope="class")
def project_with_broken_schema():
    """Project with wrong column name in generateSyntheticData."""
    tmp_dir, proj = _build_project_dir()
    try:
        _fill_stubs(proj, "scala", "cudf")
        bench_path = os.path.join(
            proj, "src", "main", "scala", "com", "udf", "bench", "BenchUtils.scala"
        )

        # Overwrite with broken source
        with open(bench_path, "r") as f:
            source = f.read()
        with open(bench_path, "w") as f:
            f.write(source.replace('.alias("value")', '.alias("wrong_column")'))

        yield proj
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCompilation:
    """Verify the export directory compiles."""

    def test_compile_smoke(self, project_dir):
        """Compile the project as-is with TODO stubs to catch basic compile errors."""
        result = run_mvn(project_dir, "clean", "compile")
        assert result.returncode == 0, "smoke compile failed"

    def test_compile_with_fixtures(self, project_with_fixtures):
        """Compile after writing UDF sources and filling stubs."""
        proj, _, _target = project_with_fixtures
        # test-compile compiles both main and test sources
        result = run_mvn(proj, "clean", "test-compile")
        assert result.returncode == 0, "compile with fixtures failed"


class TestComparisonTest:
    """Run the comparison test suite."""

    def test_run_comparison_test(self, project_with_fixtures):
        """Execute the comparison test (CudfComparisonTest or SqlComparisonTest)."""
        proj, _, target = project_with_fixtures
        suite = (
            "com.udf.SqlComparisonTest"
            if target == "sql"
            else "com.udf.CudfComparisonTest"
        )
        result = run_mvn(
            proj,
            "test",
            extra_args=[
                *(["-Pcuda-native-udf"] if target == "cuda" else []),
                f"-Dsuites={suite}",
            ],
        )
        assert result.returncode == 0, f"{suite} failed"


class TestBench:
    """Test the benchmark pipeline (GenData + BenchRunner)."""

    def test_validate(self, project_with_fixtures):
        """GenData validation: generate a small dataset and run validation."""
        proj, _, target = project_with_fixtures
        result = run_mvn(
            proj,
            "compile",
            "exec:java",
            extra_args=[
                *(["-Pcuda-native-udf"] if target == "cuda" else []),
                "-Dexec.mainClass=com.udf.bench.GenData",
                "-Dexec.classpathScope=compile",
                "-Dexec.args=--rows 1000 --validate --spark-conf spark.master=local[*]",
            ],
        )
        assert result.returncode == 0, "GenData validate failed"

    def test_spark_e2e(self, project_with_fixtures):
        """End-to-end: GenData generates data, SparkBenchRunner benchmarks CPU/GPU."""
        proj, _, target = project_with_fixtures

        data_dir = os.path.join(proj, "data", "bench_input")
        result_path = os.path.join(proj, "results", "bench_result.json")
        mvn_args = ["--mvn-arg", "-Pcuda-native-udf"] if target == "cuda" else []
        try:
            # GenData: generate parquet
            gen_result = run_script(
                os.path.join(proj, "run_gen_data.sh"),
                args=["--rows", "1000", "--output-path", data_dir, *mvn_args],
            )
            assert gen_result.returncode == 0, "run_gen_data.sh failed"

            # BenchRunner: run both benchmarks
            for mode in ["cpu", "gpu"]:
                bench_result = run_script(
                    os.path.join(proj, "run_spark_benchmark.sh"),
                    args=[
                        "--mode",
                        mode,
                        "--data-path",
                        data_dir,
                        "--result-path",
                        result_path,
                        *mvn_args,
                    ],
                )
                assert (
                    bench_result.returncode == 0
                ), f"run_spark_benchmark.sh --mode {mode} failed"
                assert os.path.isfile(
                    result_path
                ), f"Result file not created: {result_path}"
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)
            if os.path.isfile(result_path):
                os.remove(result_path)

    def test_micro_e2e(self, project_with_fixtures):
        """End-to-end: GenData generates data, MicroBenchRunner benchmarks CPU/GPU."""
        proj, _, target = project_with_fixtures
        if target not in {"cudf", "cuda"}:
            pytest.skip("MicroBenchRunner only applies to RapidsUDF targets")

        data_dir = os.path.join(proj, "data", "micro_input")
        mvn_args = ["--mvn-arg", "-Pcuda-native-udf"] if target == "cuda" else []
        try:
            # GenData: generate parquet
            gen_result = run_script(
                os.path.join(proj, "run_gen_data.sh"),
                args=["--rows", "1000", "--output-path", data_dir, *mvn_args],
            )
            assert gen_result.returncode == 0, "run_gen_data.sh failed"

            # MicroBenchRunner: run both benchmarks
            bench_result = run_script(
                os.path.join(proj, "run_micro_benchmark.sh"),
                args=["--mode", "all", "--data-path", data_dir, *mvn_args],
            )
            assert bench_result.returncode == 0, (
                "run_micro_benchmark.sh failed:\n"
                + bench_result.stdout
                + bench_result.stderr
            )
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)


class TestErrors:
    """Verify that errors are caught by the test harness."""

    def test_comparison_catches_gpu_error(self, project_with_broken_gpu):
        """Comparison test should fail when GPU implementation produces wrong results."""
        proj, target = project_with_broken_gpu
        suite = (
            "com.udf.SqlComparisonTest"
            if target == "sql"
            else "com.udf.CudfComparisonTest"
        )

        result = run_mvn(
            proj,
            "test",
            extra_args=[
                *(["-Pcuda-native-udf"] if target == "cuda" else []),
                f"-Dsuites={suite}",
            ],
        )
        assert (
            result.returncode != 0
        ), f"{suite} should have failed with broken GPU implementation"
        combined = result.stdout + result.stderr
        assert (  # 123 * 2 vs. 123 * 3, since we swapped multiplier
            "246" in combined and "369" in combined
        ), "Expected to see mismatch in test output"

        if target == "cudf":
            # Re-run with leak detection to verify the unclosed Scalar is reported
            leak_result = run_mvn(
                proj,
                "test",
                extra_args=[
                    f"-Dsuites={suite}",
                    "-Ddebug.memory.leaks=true",
                ],
            )
            assert (
                leak_result.returncode != 0
            ), f"{suite} should have failed with broken GPU implementation"
            leak_output = leak_result.stdout + leak_result.stderr
            assert "A SCALAR WAS LEAKED" in leak_output, "Expected memory leak"

    def test_bench_validate_catches_error(self, project_with_broken_schema):
        """GenData --validate should fail when synthetic data has wrong schema."""
        result = run_mvn(
            project_with_broken_schema,
            "compile",
            "exec:java",
            extra_args=[
                "-Dexec.mainClass=com.udf.bench.GenData",
                "-Dexec.classpathScope=compile",
                "-Dexec.args=--rows 1000 --validate --spark-conf spark.master=local[*]",
            ],
        )
        assert (
            result.returncode != 0
        ), "GenData --validate should have failed with wrong schema"
        assert (
            "org.apache.spark.sql.AnalysisException" in result.stderr
        ), "Expected to see schema error message"
