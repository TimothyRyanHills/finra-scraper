# CLAUDE.md — FINRA Scraper

FINRA broker-dealer firm scraper. **First full run completed 2026-04-02 — 3,053 firms scraped.** Output is the canonical snapshot used downstream by `finra-enrichment` for CEO identification + email verification.

## ⚠️ Repo housekeeping

This local clone is on a **wrong-name remote** and a **non-default branch**:

- Remote: `git@github.com:TimothyRyanHills/test-repo-1738098343442.git` (generic test name from a Claude Web session)
- Branch: `claude/finra-scraping-plan-o7w5K`
- Workflow: GitHub Actions in this repo runs the scraper

This needs cleanup — the canonical home should be `TimothyRyanHills/finra-scraper`, branch `main`. Until then, **do not assume this lives where you'd expect**.

## File layout

```
finra-scraper-repo/             ← git root (this directory)
  finra-scraper/                ← actual project code
    main.py                     ← entry point
    scraper/                    ← scraper modules
    config.py
    db.py
    export.py
    models.py
    requirements.txt
    data/
      finra.db                  ← 13 MB SQLite, resume state + canonical store
      output/
        firms.csv               ← 3,053 firms, 984 KB ← canonical
        firms.json              ← 12 MB
        firms.jsonl             ← 12 MB
  finra_scrape.log              ← scrape log
  README.md                     ← stub ("test-repo-1738098343442")
```

## Output snapshot (2026-04-02)

| File | Size | Rows |
|---|---|---|
| `finra-scraper/data/output/firms.csv` | 984 KB | **3,053 firms** |
| `finra-scraper/data/output/firms.json` | 12 MB | 3,053 firms |
| `finra-scraper/data/output/firms.jsonl` | 12 MB | 3,053 firms |
| `finra-scraper/data/finra.db` | 13 MB | full SQLite snapshot |

### CSV schema (26 columns)

```
crd_number, sec_number, name, other_names, registration_status,
registration_begin_date, firm_size, firm_type, formed_state, formed_date,
phone, finra_approved_registration_count, number_of_branches,
branch_locations_count, disclosures_count, street, city, state, zip_code,
country, latitude, longitude, matched_from_name, match_confidence, phase,
scraped_at
```

`other_names` is a **pipe-delimited** list of DBAs / branch names. For big BDs (e.g. `&PARTNERS`) this can hold hundreds of entries in a single cell.

## How to re-run (only if you need fresh data)

The scraper runs as a **GitHub Actions workflow**, not locally.

1. Open `TimothyRyanHills/test-repo-1738098343442` in GitHub
2. Actions tab → "FINRA Scraper" workflow
3. Run workflow dropdown → pick branch `claude/finra-scraping-plan-o7w5K`
4. Settings: API enumeration, all phases, 0.5s delay (defaults are fine)
5. ~37 min for the full ~3K firm enumeration
6. Artifacts auto-commit back to the branch + CSV/JSON/JSONL/SQLite saved as workflow artifacts

**Triggerable from phone** via the GitHub Actions UI — that's why this is a workflow rather than a server cron.

The local copy at `finra-scraper-repo/` is the authoritative snapshot. No need to re-run unless you want fresh data.

## Local run (not the standard path)

If you must run locally:

```bash
source /home/gg/old-server-data/ucc_env/bin/activate
cd /home/gg/finra-scraper-repo/finra-scraper
pip install -r requirements.txt    # check first whether shared venv already has them
python main.py
```

## Downstream consumers

- **`finra-enrichment/`** is the canonical downstream consumer. It takes `firms.csv` and identifies + verifies CEO emails. As of 2026-04-08: 2,394 CEOs identified, 1,735 with verified-deliverable email (72.5%). See `MEMORY.md` → `finra_enrichment.md`.
- **NOT the import workstream.** FINRA enrichment is a separate workstream from the IEEPA import scrapers — see `MEMORY.md` → `feedback_finra_not_imports.md`. Do not lump them together.
