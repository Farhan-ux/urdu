# ARY News Urdu dataset build record

## Source and scope

- Source: https://urdu.arynews.tv/
- Collection method: the site's public WordPress REST API, /wp-json/wp/v2/posts
- Archive surface observed: 3,520 paginated API requests, up to 100 published posts per page
- The API reported 351,952 published posts during the final discovery request; the local cleaned snapshot contains 351,955 unique article IDs because the live archive changed during collection.

## Local snapshot validation

- Output: compressed JSONL
- Unique records: 351,955
- Malformed records: 0
- Duplicate records removed: 1,000
- Empty-text records retained with metadata: 8,314
- Media downloaded: none

## Repository build

- Scraper code: scripts/scrape_arynews_urdu_dataset.py
- GitHub Actions workflow: .github/workflows/build-arynews-dataset.yml
- Generated dataset folder: data/arynews_urdu/
- The workflow regenerates the archive, splits the compressed file into 90 MB repository-safe parts, and commits those parts.
- Reassembly instructions are in data/arynews_urdu/README.md.

No passwords, API keys, or personal access tokens are stored in this repository. GitHub authentication is supplied by the repository's Actions token at run time.
