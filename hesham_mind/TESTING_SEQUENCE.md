# DVT Testing Sequence

> **Rule**: Never publish without passing both editable AND live mode tests.

---

## Sequence

```
1. EDITABLE MODE (source code)
   ├── Trial 20 in editable_mode/
   ├── Uses: [tool.uv.sources] dvt-ce = { path = "...", editable = true }
   ├── Tests: dvt debug, seed, run, run --full-refresh, test, docs, retract
   ├── Catches: logic bugs, missing decorators, import errors
   └── PASS? → Step 2

2. COMMIT + PUSH
   ├── git add + commit on new_dev
   ├── git push origin new_dev
   └── Ready for publish

3. PUBLISH
   ├── Bump version in setup.py + version.py
   ├── git tag v0.x.y && git push origin v0.x.y
   ├── GitHub Actions builds Cython wheels (macOS x86+arm, Linux x86)
   ├── Publishes to PyPI automatically
   └── Wait for build to succeed on GitHub Actions

4. LIVE MODE (compiled from PyPI)
   ├── Trial 20 in live_mode/
   ├── Uses: dependencies = ["dvt-ce"] (no editable, from PyPI)
   ├── uv sync -U (gets new version from PyPI)
   ├── Tests: same as editable mode
   ├── Catches: Cython type strictness, missing assets, packaging issues
   ├── Also verifies: PostHog audit events appear in dashboard
   └── PASS? → Step 5

5. RELEASE VERIFIED
   ├── git branch -f uat new_dev && git branch -f master new_dev
   ├── Force push uat + master
   └── Done — version is production-ready
```

---

## Test Checklist (both editable and live mode)

### Core Commands
- [ ] `dvt debug --project-dir Coke_DB` → 9/9 connections OK (Adapter + Sling)
- [ ] `dvt seed --project-dir Coke_DB` → 11/11 seeds PASS
- [ ] `dvt run --full-refresh --project-dir Coke_DB` → 68/68 PASS
- [ ] `dvt run --project-dir Coke_DB` (incremental) → 68/68 PASS
- [ ] `dvt test --project-dir Coke_DB` → tests run without crash
- [ ] `dvt compile --project-dir Coke_DB` → compiles without error
- [ ] `dvt docs generate --project-dir Coke_DB` → catalog with 28 sources + 68 models with columns
- [ ] `dvt retract --select customer_1 --project-dir Coke_DB` → drops model cleanly

### DVT-Specific Commands
- [ ] `dvt init` (no name) → scaffolds in current directory
- [ ] `dvt init my_project` → creates subdirectory
- [ ] `dvt sync` → installs drivers, DuckDB extensions
- [ ] `dvt show --select transactions_1 --project-dir Coke_DB` → DuckDB query works

### Audit (live mode only)
- [ ] PostHog dashboard → events appear (invocation_start, run_model, dvt_debug, etc.)

### Error Handling
- [ ] Without psycopg2: `dvt parse` → clean "Run dvt sync" error, no traceback
- [ ] Missing drivers: `dvt sync` → installs them

---

## Folder Structure

```
Testing_Factory/Testing_Playground/
├── editable_mode/
│   ├── trial_20_full_coverage/     ← source code testing
│   │   ├── Coke_DB/
│   │   └── pyproject.toml          ← has [tool.uv.sources] editable
│   ├── trial_19_dvt_ce_pypi/
│   └── ... (other trials)
│
└── live_mode/
    └── trial_20_full_coverage/     ← PyPI compiled testing
        ├── Coke_DB/
        └── pyproject.toml          ← NO [tool.uv.sources], plain deps
```

---

## When to Run Each

| Trigger | Editable | Live |
|---------|----------|------|
| Bug fix | Yes | After publish |
| New feature | Yes | After publish |
| Version bump only | Skip | Yes |
| Pre-release verification | Yes | Yes |
| User reports PyPI issue | Skip | Yes |
