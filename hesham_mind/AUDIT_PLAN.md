# DVT Telemetry Plan — PostHog Integration

> **Status**: PLANNED
> **PostHog API Key**: `phc_B9D5fqEjUY6qkApj8ekAc5FnAxVudwT7bvVYzTnxb7yb`
> **PostHog Host**: `https://us.i.posthog.com`

---

## What Exists Today

dbt-core has a Snowplow tracker in `core/dbt/tracking.py` that sends anonymous usage events to `fishtownanalytics.sinter-collect.com` (dbt Labs' collector). This means dbt Labs gets DVT usage data — not us.

## What We're Doing

Replace Snowplow with PostHog. Same anonymous tracking, same opt-out, but data goes to OUR PostHog dashboard.

## Architecture

```
User runs "dvt run"
    ↓
tracking.py collects anonymous event:
  - command: "run"
  - edition: "ce" (or "pro" in future)
  - dvt_version: "0.1.40"
  - adapter_types: ["postgres", "snowflake"]
  - execution_path: "extraction"
  - model_count: 68
  - duration_seconds: 120
  - success: true
  - os: "Linux-5.15.0"
  - python_version: "3.12.10"
  - user_id: "a1b2c3d4-..." (UUID from ~/.dvt/.user.yml)
    ↓
posthog.capture(user_id, "dvt_run", properties)
    ↓
PostHog dashboard (us.i.posthog.com)
    ↓
We see: users, commands, adapters, errors, growth
```

## Events Tracked

### Inherited from dbt (rewritten for PostHog)

| Event | When | Key Properties |
|-------|------|---------------|
| `invocation_start` | Any command starts | command, version, adapter_type, platform |
| `invocation_end` | Any command ends | command, result_type (ok/error), duration |
| `run_model` | Each model executes | materialization, status, execution_time |
| `resource_counts` | After parsing | models, sources, seeds, tests counts |
| `adapter_info` | Profile loaded | adapter_type |
| `project_id` | Project identified | hashed project name |

### DVT-Specific (NEW)

| Event | When | Key Properties |
|-------|------|---------------|
| `dvt_extraction` | Model uses extraction path | execution_path, adapter_types, model_count |
| `dvt_seed` | Seeds loaded via Sling | seed_count, adapter_type |
| `dvt_sync` | Environment bootstrap | adapter_types, drivers_installed |
| `dvt_debug` | Connection test | adapter_ok, sling_ok per connection |
| `dvt_retract` | Models dropped | model_count, adapter_types |
| `dvt_docs` | Docs generated | source_count, engine_count |
| `dvt_init` | Project initialized | adapter_type selected |

### Common Properties (every event)

```python
{
    "edition": "ce",          # "ce" or "pro"
    "dvt_version": "0.1.40",
    "invocation_id": "uuid",
    "os": "Linux-5.15.0",
    "python_version": "3.12.10",
}
```

## Privacy & Opt-Out

- **Anonymous UUID**: stored in `~/.dvt/.user.yml` (not tied to name/email)
- **Hashed data**: model names → MD5, project names → MD5
- **NO collection of**: SQL code, credentials, file paths, data content
- **Opt-out**: `--no-send-anonymous-usage-stats` flag, or `DVT_SEND_ANONYMOUS_USAGE_STATS=false` env var
- **API key protection**: embedded in compiled Cython .so — not visible in source

## Implementation

### Files to Modify

| File | Change |
|------|--------|
| `core/dbt/tracking.py` | Replace Snowplow with PostHog (complete rewrite) |
| `core/setup.py` | Replace `snowplow-tracker` dep with `posthog` |
| `core/dvt/tasks/run.py` | Add `track_dvt_extraction` call |
| `core/dvt/tasks/seed.py` | Add `track_dvt_seed` call |
| `core/dvt/tasks/sync.py` | Add tracking init + `track_dvt_sync` call |
| `core/dvt/tasks/debug.py` | Add `track_dvt_debug` call |
| `core/dvt/tasks/retract.py` | Add `track_dvt_retract` call |
| `core/dvt/tasks/init.py` | Add `track_dvt_init` call |
| `core/dvt/cli/main.py` | Add `track_dvt_docs` call in docs_generate |

### Key Design Decisions

1. **Replace Snowplow, don't dual-track**: We don't want data going to dbt Labs
2. **PostHog Python SDK**: `posthog.capture()` is much simpler than Snowplow's SelfDescribingJson
3. **Same function signatures**: All existing `track_*` functions keep their signatures — callers unchanged
4. **Edition field**: Every event includes `edition: "ce"` — when dvt-pro ships, it sends `edition: "pro"` using the same tracking code (single codebase, feature flags)
5. **Sync command special handling**: `dvt sync` doesn't go through dbt's `requires.preflight`, so tracking init must be done manually in the sync task

## What We See in PostHog

After deployment, the PostHog dashboard shows:
- **Daily/weekly active users** (unique UUIDs)
- **Command frequency** (which commands are most used)
- **Adapter distribution** (postgres 60%, snowflake 20%, etc.)
- **Extraction vs pushdown** ratio
- **Error rates** by command
- **Version adoption** (how fast users upgrade)
- **Platform distribution** (Linux vs Mac vs Windows)
- **Growth trends** over time
- **Funnel**: init → sync → debug → run → docs (user journey)
