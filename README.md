# finra-scraper

FINRA broker-dealer firm scraper. First full run completed 2026-04-02 — **3,053 firms** scraped, output is the canonical snapshot used downstream by `finra-enrichment` for CEO identification + email verification.

See [`CLAUDE.md`](CLAUDE.md) for full project documentation.

## Quick links

- **Output:** `finra-scraper/data/output/firms.csv` (3,053 firms, 26 columns)
- **Re-run:** GitHub Actions → "FINRA Scraper" workflow → Run workflow on `main`
- **Downstream consumer:** [`TimothyRyanHills/finra-enrichment`](https://github.com/TimothyRyanHills/finra-enrichment)
