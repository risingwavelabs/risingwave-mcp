## Summary

<!-- 1-3 sentences describing what this PR changes and why. -->

## Python compatibility checklist

See `AGENTS.md` for the invariants this checklist enforces.

- [ ] Verified that any new syntax compiles on the minimum supported Python (`python3.10 -m compileall -q src`).
- [ ] If `requirements.txt` was changed, verified each new/updated dep's PyPI `Requires-Python` still permits the declared floor in `pyproject.toml`.
- [ ] If the declared Python floor changed, `pyproject.toml`, README, and the CI matrix were updated together in this PR.
- [ ] `python -c "import sys; sys.path.insert(0, 'src'); import main"` still succeeds (import-time startup is a hard invariant).

## Test plan

<!-- How did you verify this works? Be concrete. -->
