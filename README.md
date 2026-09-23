# Urdu NLP Corpus

A large-scale Urdu dataset for NLP research, targeting 5M+ documents from Urdu news sites and other sources.

## Repository Structure

```
urdu/
├── notebooks/
│   └── arynews_scraper.ipynb     # Google Colab notebook for scraping arynews.tv
├── scripts/                       # Standalone Python scripts (mirror of notebook code)
│   └── arynews_scraper.py
└── README.md
```

## Scraping Strategy

For each site, a separate Colab notebook is provided. The workflow is:
1. **URL discovery** — walk category pagination concurrently to find all article URLs
2. **Article fetch** — scrape articles concurrently with retries and backoff
3. **Checkpointing** — save state to Google Drive every 30 minutes so interrupted sessions can resume
4. **Output** — gzip JSONL shards on Google Drive, each containing 500 articles

## Sites Planned

| Site | Status | Notes |
|------|--------|-------|
| arynews.tv | ✅ Notebook ready | 10 categories, ~200K+ articles expected |
| geo.tv | ⏳ Planned | |
| dawnnews.tv | ⏳ Planned | |
| express.com.pk | ⏳ Planned | |
| jang.com.pk | ⏳ Planned | |
| ... | | (15-20 sites total to reach 5M) |

## Using the arynews Scraper

1. Open `notebooks/arynews_scraper.ipynb` in Google Colab
2. Run the cells in order:
   - Cell 1: install dependencies
   - Cell 2: mount your Google Drive
   - Cell 3: load scraper code
   - Cell 4: start scraping (leave running up to 12 hours)
3. If Colab disconnects, just re-run cells 1-4 — the scraper resumes from where it stopped
4. Cell 5: check progress / verify output anytime

### Output Schema

Each line in the gzip JSONL output is a JSON object with:

```json
{
  "url": "https://urdu.arynews.tv/...",
  "title": "...",                  // Urdu article title
  "category": "...",               // Urdu category name (e.g. پاکستان)
  "published_date": "2026-09-23T11:06:01+05:00",
  "body_text": "...",              // Full article body in Urdu
  "char_count": 848,
  "scraped_at": "2026-09-23T07:50:01.423214+00:00"
}
```

### Categories Scraped from arynews.tv

| # | Urdu | English | Articles (est.) |
|---|------|---------|-----------------|
| 1 | پاکستان | Pakistan | 50,000+ |
| 2 | عالمی خبریں | World | TBD |
| 3 | کھیل | Sports | TBD |
| 4 | حیرت انگیز | Amazing | TBD |
| 5 | تجارت | Business | TBD |
| 6 | صحت | Health | TBD |
| 7 | فن و ثقافت | Arts & Culture | TBD |
| 8 | میگزین | Magazine | TBD |
| 9 | سائنس اور ٹیکنالوجی | Science & Tech | TBD |
| 10 | بلاگز | Blogs | TBD |

(Multimedia category excluded — video/photo galleries only, no article text.)

## License

The scraping code is MIT-licensed. The scraped article content remains the copyright of the respective publishers and is intended for research use under fair use / similar provisions.
