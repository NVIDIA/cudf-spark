# Testing

The commands assume you are in the `skills/` directory.

## Setup

Set up a local dev environment:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Fast Tests

Run the fast tests:

```bash
pytest -m "not slow"
```

These are generally lightweight skill validation tests, such as verifying skill frontmatter.

## Integration Tests

Run the integration tests:

```bash
pytest -m slow -s
```

These tests deterministically fill in the template project from `skills/udf-gen-test/templates/` with fixture implementations, then actually compile and run Spark tests and benchmark scripts locally.

Thus they require JDK, Maven and Maven repository access, a GPU environment, and (for `cuda` tests) CMake and CUDA toolkit.
