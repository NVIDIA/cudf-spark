# Maintaining the Maven cache

- Keep [action.yml](action.yml), the [warmer](../../workflows/cache.yml) and
  the Verify/GPU consumers on the same branch, architecture, Scala and dependency
  fingerprint. A smaller Verify cache does not satisfy an IT runtime seed.
- Do not reuse stale mutable SNAPSHOTs or PR-built project artifacts. A restore
  failure must leave a clean repository and allow the real build to run.
- Passing a target branch name changes the key, not GitHub's cache visibility.
  In particular, a copied branch cannot read a sibling release branch's cache.
- [settings.xml](settings.xml) configures Maven; Spark `--packages` uses
  [ivysettings.xml](ivysettings.xml). Test both warm and cold dependency paths.
- Run `python3 -m unittest discover -s .github/actions/maven-cache`.
