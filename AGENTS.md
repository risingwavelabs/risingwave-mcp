# AGENTS.md

Repo-level constraints that agents (and humans) must honor when editing
this codebase. Keep this file short and actionable.

## 1. Python floor is **3.10+**, bounded by dependencies

The supported Python range is set by what the dependencies in
`requirements.txt` can install, **not** by stylistic preference.
Currently, `fastmcp` has `Requires-Python >=3.10`, so the floor is
**Python 3.10**.

**Before lowering** `requires-python` / `python = "^X.Y"` in
`pyproject.toml`, verify every dependency's PyPI metadata supports the
new floor:

```bash
for pkg in $(awk -F'[<>=!~ ]' '{print $1}' requirements.txt); do
  python -c "import urllib.request, json; \
    d = json.loads(urllib.request.urlopen(f'https://pypi.org/pypi/$pkg/json').read()); \
    print(f'$pkg', d['info'].get('requires_python', 'unknown'))"
done
```

When a dependency raises its floor, update `pyproject.toml`, README,
and the CI matrix **in the same PR**. Don't let declared support drift
above the real install floor.

## 2. Syntax must compile on the lowest supported version

Local Python is not the source of truth — the CI matrix is. Common
gotchas that compile on newer Python but fail on the floor:

- f-string expression part containing `\` (allowed only from 3.12+, PEP 701)
- PEP 604 union syntax `X | Y` in type hints (allowed from 3.10+)
- `typing.Self` (allowed from 3.11+)
- `tomllib` (stdlib only from 3.11+)

**Verify before opening a PR**:

```bash
python3.10 -m compileall -q src   # use pyenv/docker if local lacks 3.10
```

`ruff` is configured with `target-version = "py310"` so the linter
will also catch these statically. Run `ruff check src/` before pushing.

## 3. `import main` is a hard invariant

A startup-time import failure means the MCP server cannot start, so
**every tool is broken at once**. CI enforces that

```bash
python -c "import sys; sys.path.insert(0, 'src'); import main"
```

succeeds on every supported Python version. Any change that breaks
this smoke test is a release blocker. If you add new top-level imports
in `src/main.py` or any module it transitively loads, run the smoke
test locally first.
