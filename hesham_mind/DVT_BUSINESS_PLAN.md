# DVT Business Plan — Closed Source + Marketing

> **Decision**: DVT-CE is closed source. GitHub repo goes private. Distribution via PyPI only (compiled Cython wheels). Marketing drives adoption.

---

## Distribution Model

```
User discovers DVT
    ↓ (website, LinkedIn, Discord, YouTube, blog posts)
DVT website (getdvt.com / dvt.dev)
    ↓ (docs, getting started, pricing)
pip install dvt-ce
    ↓ (compiled .so wheels from PyPI — no source code)
dvt sync → dvt run → dvt docs
    ↓ (telemetry → PostHog)
Usage data flows back to us
```

### What users see:
- **Website**: landing page, docs, getting started, pricing, Discord link
- **PyPI**: `pip install dvt-ce` (compiled wheels, no .py source)
- **Discord**: community support, announcements
- **Docs**: hosted on the website (not GitHub)

### What users DON'T see:
- Source code (GitHub is private)
- Internal architecture docs (hesham_mind/)
- CI/CD pipeline

---

## Package Architecture (Option A — Feature Flags)

**Single codebase, single repo, single CI/CD:**

```
dvt-ce (private GitHub repo)
├── Free features (always available)
│   ├── dvt run, build, seed, test, compile, snapshot
│   ├── dvt init, sync, debug, show, docs, retract, clean
│   ├── All 13 adapters (via dvt-adapters)
│   ├── Sling free tier (full-refresh, truncate, incremental)
│   ├── DuckDB cache + federation optimizer
│   ├── Cross-engine extraction
│   ├── Engine-colored lineage docs
│   └── Telemetry (PostHog, anonymous, opt-out available)
│
├── Pro features (locked behind DVT_PRO_KEY)
│   ├── Sling Pro (parallel streams, API sources, CDC)
│   ├── Bucket materialization (S3/GCS/Azure as Delta/Parquet)
│   ├── Advanced incremental strategies
│   ├── Priority support
│   └── [Future: scheduling, RBAC, audit logs]
│
└── License validator
    ├── Checks DVT_PRO_KEY env var or ~/.dvt/license.key
    ├── Validates against license server (or offline key)
    └── Compiled in Cython (.so) — can't be bypassed
```

**dvt-pro on PyPI** = thin wrapper:
```python
# dvt-pro just sets the license key and depends on dvt-ce
setup(
    name="dvt-pro",
    install_requires=["dvt-ce>=0.1.39"],
    # Post-install script stores license key
)
```

---

## Revenue Model (Future — Not Implemented Now)

| Tier | Price | What You Get |
|------|-------|-------------|
| **DVT CE** | Free | All core features, 13 engines, Sling free tier |
| **DVT Pro** | $X/month per user | Sling Pro, bucket targets, CDC, priority support |
| **DVT Enterprise** | Custom | Self-hosted, SSO/RBAC, dedicated support, SLA |

**Pricing TBD** — depends on Sling Pro pricing and market research.

---

## Marketing Channels

### Immediate (Week 1-4)
1. **LinkedIn** — Hesham's profile, posts about DVT, cross-engine federation demos
2. **Discord** — Community server (already have: https://discord.gg/UjQcxJXAQp)
3. **DVT website** — Landing page, getting started guide, video demo
4. **PyPI description** — README with Mermaid diagram, badges (already done)
5. **Reddit** — r/dataengineering, r/analytics, r/dbt posts
6. **Twitter/X** — Data engineering community

### Medium-term (Month 2-6)
7. **YouTube** — Tutorial videos: "Cross-engine joins in 5 minutes"
8. **Blog posts** — "Why we built DVT", "DVT vs dbt", "Federated queries without Spark"
9. **Hacker News** — "Show HN: DVT — cross-engine data transformation tool"
10. **dbt Slack** — Tasteful mentions where relevant (not spam)
11. **Conference talks** — Data Council, dbt Coalesce, local meetups
12. **Product Hunt** — Launch day

### Long-term (Month 6+)
13. **Case studies** — Real companies using DVT
14. **Partnerships** — Sling, DuckDB, database vendors
15. **SEO** — Blog content targeting "cross-engine SQL", "federated queries", "dbt alternative"
16. **Paid ads** — Google Ads for "dbt alternative", "cross-database ETL"

---

## Telemetry (PostHog)

### What We Track (CE)
- Command usage (run, build, seed, test, sync, debug, docs, retract, init, show)
- Adapter types used (postgres, snowflake, databricks, etc.)
- Execution path distribution (pushdown vs extraction)
- Model counts, source counts
- Error rates by command
- OS + Python version
- Anonymous user ID (UUID)
- DVT version

### What We Track (Pro — Future)
- All CE events + `edition: "pro"`
- Pro feature usage (parallel streams, CDC, bucket targets)
- License key validation events
- Subscription status

### Privacy
- All data anonymized (hashed model names, no SQL, no credentials)
- Opt-out: `--no-send-anonymous-usage-stats` or env var
- GDPR compliant (PostHog EU region available)

---

## Roadmap Priority

### Phase 1: Foundation (NOW)
1. ✅ Cython compilation (GitHub Actions CI/CD)
2. 🔲 PostHog telemetry integration
3. 🔲 Make GitHub repo private
4. 🔲 DVT website (landing page + docs)

### Phase 2: Growth (Month 1-3)
5. 🔲 Marketing push (LinkedIn, Reddit, HN, YouTube)
6. 🔲 Unit + integration tests
7. 🔲 License validation framework (for future pro features)
8. 🔲 Bucket materialization (P15.1)

### Phase 3: Monetization (Month 3-6)
9. 🔲 DVT Pro features (Sling Pro, CDC, parallel streams)
10. 🔲 Pricing + payment (Stripe integration in CLI)
11. 🔲 Enterprise tier (self-hosted, SSO)

---

## GitHub Repo Transition

### Steps to go private:
1. Ensure all CI/CD (GitHub Actions) works with private repo
2. Move docs content to website
3. Update all links (README, PyPI, Discord) to point to website instead of GitHub
4. Go to https://github.com/heshamh96/dvt-ce/settings → Change visibility → Private
5. dvt-adapters: also go private (or keep public — adapters are less sensitive)
6. Remove any GitHub links from CLI output, error messages, etc.

### What stays public:
- PyPI packages (dvt-ce, dvt-adapters, dvt-pro)
- DVT website
- Discord community
- Documentation

---

## Key Files Reference

| Item | Location |
|------|----------|
| CI/CD | `.github/workflows/build-wheels.yml` |
| Setup | `core/setup.py` |
| Telemetry | `core/dbt/tracking.py` (to be modified) |
| CLI | `core/dvt/cli/main.py` |
| Cython exclude list | `core/setup.py` (EXCLUDE_FROM_CYTHON) |
| Business plan | `hesham_mind/DVT_BUSINESS_PLAN.md` (this file) |
