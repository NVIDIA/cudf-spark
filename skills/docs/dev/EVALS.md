# Evals

Each skill ships an `evals/` directory that the **NVSkills-Eval** pipeline runs before a skill is published to the [NVIDIA/skills](https://github.com/nvidia/skills) catalog. The pipeline measures whether a skill is safe, discoverable, and actually helps an agent.

## TLDR

- Each skill is evaluated in CI by running an agent on a set of tasks, with an LLM-as-a-judge scoring the result against user-defined criteria.
- A per-skill `evals/` directory defines the environment (config + container) and the eval cases with their expected behavior and criteria.
- We provide a common set of code fixtures in `evals/resources/`, synced into every skill's `evals/` so each skill is evaluated against the same artifacts.

## How evaluation works

For each eval case, the pipeline runs an agent (e.g. Claude Code) inside a container using [Harbor](https://github.com/harbor-framework/harbor). The agent is run **with the skill available** and, optionally, a baseline run **without the skill**. It collects the full agent trajectory and then uses an LLM-as-a-judge across five dimensions:

| Dimension | What it checks |
| --- | --- |
| **Security** | The skill run avoids unsafe behavior (secret leakage, destructive commands, unauthorized access). |
| **Correctness** | The agent produced a correct, working result for the task. |
| **Discoverability** | The agent loaded the right skill for the prompt (and did *not* load it for unrelated prompts). |
| **Effectiveness** | The skill meaningfully helped the agent complete the task. |
| **Efficiency** | The task was completed without excessive or wasteful steps. |

The **with-skill** score for each dimension must clear a pass threshold for the skill to pass. The **lift** (with-skill vs. without-skill) is reported for insight but is not the gate. Results are summarized in a generated `BENCHMARK.md` per skill.

The judging is driven entirely by the contents of each skill's `evals/` directory, described below.

### Limitations

The default LLM judges never inspects the on-disk produced files. They are only given a **budget-limited summary** of the agent's trajectory. Tool-call arguments, e.g. `Write`/`Edit` file content, are heavily truncated.

This means that `expected_output` and `assertions`, by default, can only target outcomes the agent surfaces in its output, not internal details of the generated code. Verifying the artifacts directly requires a custom grader script.

This is left as a limitation for now. A custom grader should be added in the future.

## Per-skill `evals/` directory

Example:

```text
skills/<skill>/evals/
├── evals.json            # eval cases
├── config.yml            # container + cross-skill staging settings
├── environment/
│   └── Dockerfile        # the container image the agent runs in (generated — see Syncing)
└── files/                # artifacts staged into agent workspace (generated — see Eval Resources)
```

### `evals.json`

Follows the [agentskills.io evals](https://agentskills.io/skill-creation/evaluating-skills) dataset shape: a `skill_name` and a list of `evals`, with a list of fields:

| Field | Meaning |
| --- | --- |
| `id` | Unique case id, conventionally `<skill>__<case>`. |
| `prompt` | The user request handed to the agent. |
| `expected_skill` | The skill that *should* activate, or `null` for a **negative** case where no skill should load. |
| `expected_script` | Optional expected script/command; usually `null`. |
| `files` | Workspace files staged for the agent before it runs (paths under `evals/files/`). |
| `expected_output` | Prose description of a correct result. Feeds the accuracy/correctness judge. |
| `assertions` | A checklist of behaviors the judge verifies step-by-step. Feeds the behavior judge. |

Each skill includes at least one **functional** case (a real task, with `expected_skill` set and the minimal input files staged) and one or more **negative** cases (for which the skill must *not* activate). To save time, negative cases should ideally be shorter single-response questions.

### `config.yml`

```yaml
schema_version: 1
harbor:
  custom_dockerfile_mode: preserve    # default is `rebase`
skill_workspace:
  mode: group              # default is `isolated`
  include:
    - ../udf-gen-test      # prerequisite skills staged alongside the skill under test
```

The config file describes how to stage the Harbor evaluation run. Notable decisions:

- `preserve`: ensures that our base Dockerfile image is built as written, rather than rebased on the default skill image.

- `mode: group` and `include`: allows additional skills to be staged alongside a skill eval. This is important because many skills depend on the initial project template in `udf-gen-test`, and assume that the project is available.

## Eval Resources

The `evals/resources/` directory contains code files provided to the agent during skill evaluations. These are staged into the `files/` directory described above.

### Purpose

While the CI evaluates each skill independently, several of the skills require artifacts from a previous step - for instance, `udf-convert-to-{x}` expects a completed unit test, `udf-benchmark` expects a working GPU implementation to benchmark. Thus, we place pre-provided artifacts under `evals/files/`, which will end up in the agent's workspace.

Some artifacts are intentionally manufactured to elicit certain behavior. E.g., `array-segment-sum/judge/` includes weak tests with limited edge-case coverage and a comparison test that runs in explain-only mode. We expect the judge to flag these issues.

### Syncing

Several skill evals share the same input files. To avoid manually editing all of these files, the `evals/resources/` directory contains the *single source of truth* for these files, and `scripts/sync_eval_resources.py` copies the files into each skill's `evals/` directory according to a manifest. Therefore, the files under `udf-*/evals/files` are generated and should *not be edited*.

The same script also copies `ci/Dockerfile.pre-merge` into each skill's `evals/environment/Dockerfile`, so that one canonical Dockerfile drives every eval environment.

After making edits to files in `evals/resources/` (or to `ci/Dockerfile.pre-merge`), the script `scripts/sync_eval_resources.py` should be re-run so that the eval artifacts are up to date.
