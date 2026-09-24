# ARY News Urdu NLP Dataset — Complete Build Report

This document records how the dataset was discovered, crawled, cleaned, validated,
compressed, and uploaded. It is written so the same approach can be adapted to
another WordPress news site.

## 1. Deliverables

- Dataset: `arynews_urdu_articles_clean.jsonl.gz`
- Public download page: https://gofile.io/d/sQYLzEZA
- Local MD5: `9c5adfeadb3a4872853898910be01b70`
- Local SHA-256:
  `d99bfda34543d2e0e749b569918e9a143a6a255a2f569861e25f11c5962a29d6`
- Compressed size: `250,854,365` bytes, approximately 240 MiB
- Format: gzip-compressed JSON Lines (JSONL)
- One JSON object is stored per line.

The public link is an anonymous temporary file-hosting link. Anyone who has the
link may download the file, and retention policies are controlled by the hosting
service rather than by this project.

## 2. Source and discovery

Source site:

    https://urdu.arynews.tv/

The site exposed a public WordPress REST API:

    https://urdu.arynews.tv/wp-json/wp/v2/posts

The site’s `robots.txt` declared the public site crawlable and advertised a
sitemap. The sitemap requests were slow or returned access errors from this
runtime, so the WordPress API was used instead. This was more complete and
stable than discovering article links from the home page and category pages.

The API’s published-post count changed during the crawl:

- Initial observation: 351,946 published posts
- Later observation: 351,948 published posts
- Final observation: 351,952 published posts
- Cleaned local snapshot: 351,955 unique article IDs

The difference is expected for a live news site: articles were published or
changed while the multi-stage crawl was running, and the first interrupted run
also produced overlapping records that were removed during validation.

## 3. Exact API request shape

Each archive page used this request pattern:

    https://urdu.arynews.tv/wp-json/wp/v2/posts?page=PAGE&per_page=100&orderby=id&order=asc&status=publish&_fields=id,date,modified,link,slug,title,content,excerpt,categories,tags,author

The actual URL was percent-encoded by the Python URL builder.

Important parameters:

- `page`: sequential archive page number
- `per_page=100`: the WordPress API maximum page size
- `orderby=id&order=asc`: stable ordering for a long crawl
- `status=publish`: published articles only
- `_fields`: limits the response to fields needed for the NLP dataset

The initial response headers exposed the archive size through:

    X-WP-Total
    X-WP-TotalPages

The final archive surface had 3,520 pages. The last page contained the remaining
records after the full 100-record pages.

## 4. Request behavior and network details

The scraper used:

- User-Agent:
  `ResearchDatasetBot/1.0 (public Urdu NLP dataset; respects robots.txt)`
- Request timeout: 90 seconds
- Maximum retry attempts per page: 6
- Retry delay: exponential backoff, capped at 60 seconds
- HTTP concurrency: 3 workers in the local crawl
- No rotating proxies
- No deliberate IP rotation
- No residential proxy network
- No browser automation
- No cookies or login session
- No API key or personal access token

The requests used the execution environment’s normal shared outbound network
egress. A list of public IP addresses was not captured, and internal/shared
egress IPs are intentionally not disclosed. There was no attempt to evade rate
limits or rotate IPs.

### API-call accounting

The crawler ultimately wrote all 3,520 archive pages. The process had to be
restarted twice after the background runtime terminated it, and its page
checkpoint allowed it to resume instead of starting over.

The logs preserve page progress and checkpoint boundaries, but not every
individual HTTP retry attempt. Therefore the exact total number of low-level
TCP/HTTP attempts cannot be reconstructed honestly. The reliable counts are:

- Archive pages represented in the completed result: 3,520
- Maximum records represented by those pages: 352,000
- Process launches of the resumable crawler: 3
- Duplicate records removed after overlapping resume output: 1,000
- Extra discovery/retry calls: present, but exact retry count not persisted

The initial site inspection also made small, bounded requests for `robots.txt`,
the sitemap, the home page, the REST API, post types, and taxonomies. Those
inspection requests are not included in the 3,520 archive-page count.

## 5. Article extraction

The WordPress API returns article title, excerpt, and body as HTML. The scraper
converted those fields to plain Unicode text using Python’s standard-library
`html.parser.HTMLParser`.

The extractor:

1. Decodes HTML character references.
2. Drops content inside `script`, `style`, `noscript`, `svg`, and `template`.
3. Inserts line breaks around paragraph, heading, list, table, figure, and other
   block-level elements.
4. Converts non-breaking spaces to ordinary spaces.
5. Collapses repeated horizontal whitespace.
6. Removes spaces around line breaks.
7. Collapses three or more consecutive blank lines to two.
8. Strips leading and trailing whitespace.
9. Preserves Urdu Unicode characters and punctuation.

No translation, transliteration, stemming, lemmatization, language model
rewriting, or semantic filtering was applied. The text is a cleaned extraction
of the public article HTML, not generated text.

Images, videos, audio, thumbnails, and other media were not downloaded.

## 6. Output schema

Each JSONL line has this structure:

```json
{
  "id": 12345,
  "url": "https://urdu.arynews.tv/article-slug/",
  "slug": "article-slug",
  "published_at": "2020-01-01T12:34:56",
  "modified_at": "2020-01-01T12:40:00",
  "title": "Urdu article title",
  "text": "Plain Urdu article body...",
  "excerpt": "Plain Urdu excerpt...",
  "categories": [1, 2],
  "tags": [10, 20],
  "author_id": 7,
  "source": "ARY News Urdu"
}
```

Field meanings:

- `id`: WordPress post ID
- `url`: canonical article URL returned by the API
- `slug`: WordPress slug
- `published_at`: original WordPress publication timestamp
- `modified_at`: last WordPress modification timestamp returned by the API
- `title`: HTML-free Urdu title
- `text`: HTML-free Urdu article body
- `excerpt`: HTML-free WordPress excerpt
- `categories`: WordPress category IDs
- `tags`: WordPress tag IDs
- `author_id`: WordPress author ID
- `source`: fixed source label

Category, tag, and author names were not separately resolved. The IDs preserve
the site’s taxonomy references without adding thousands of additional lookup
requests.

## 7. Final validation results

The final cleaned file was streamed from gzip and parsed line by line.

- Total JSONL records: 351,955
- Unique article IDs: 351,955
- Malformed JSON records: 0
- Duplicate records remaining: 0
- Duplicate records removed: 1,000
- Records with empty `text`: 8,314
- Records with empty text were retained because their metadata may still be
  useful and because dropping them would no longer represent every discovered
  published post.
- Article body/title text characters counted: 452,090,413
- Lowest article ID: 3,461
- Highest article ID: 988,688
- Earliest observed publication timestamp:
  `2013-12-17T14:43:46`
- Latest observed publication timestamp:
  `2026-09-23T20:29:33`
- gzip integrity test: passed

The file was deduplicated by the WordPress `id` field, keeping the first
complete JSON record encountered for each ID.

## 8. Runtime specifications

The crawl ran in a Linux x86_64 Replit workspace.

Observed runtime information:

- Operating system kernel:
  `Linux 6.18.52 #Replit-Linux SMP Mon Sep 14 11:36:19 UTC 2026 x86_64`
- Python: `3.13.11`
- Visible logical CPUs: 1
- Visible memory: approximately 1.5 GiB
- Swap: 0 bytes
- Workspace filesystem: 256 GB
- Workspace free space at inspection: approximately 254 GB
- GPU: not used
- Database: not used
- External queue: not used

The crawler was I/O-bound. It used three HTTP worker threads even though the
workspace exposed one logical CPU. Article parsing used the Python standard
library and did not require a large model or GPU.

## 9. Resilience and interruption handling

The first long-running background process was terminated by the runtime. The
scraper was designed to resume:

- A checkpoint file stored completed page numbers.
- Completed pages were appended as separate gzip members.
- Each page was flushed and synced before being checkpointed.
- A restarted process skipped checkpointed pages.
- The interrupted stream was repaired before resuming.
- The final concatenated gzip file passed `gzip -t`.
- Overlapping records caused by the interruption were removed by ID.

The checkpoint file was removed after the final clean dataset was produced.

## 10. Reproducing the scraper locally

The scraper is self-contained and uses only Python’s standard library. No
`pip install` step is required.

Run:

```bash
python scrape_arynews_urdu.py \
  --output arynews_urdu_articles.jsonl.gz \
  --workers 3
```

Useful options:

```text
--output       Output gzip JSONL path
--workers      HTTP worker count, from 1 through 6
--per-page     API page size, from 1 through 100
--start-page   Page from which to begin/resume
```

For a different WordPress news site, change:

1. `API` to the site’s REST posts endpoint.
2. `USER_AGENT` to identify the research crawler.
3. `FIELDS` if the site exposes different fields.
4. The record mapping in `record()`.
5. The source label.
6. The output and checkpoint names.

For a non-WordPress site, replace `get_page()` with a sitemap, archive,
GraphQL, RSS, or site-specific pagination reader. Preserve stable ordering,
retries, checkpoints, and a source URL in every record.

## 11. Recommended adaptation workflow for another site

1. Inspect `robots.txt` and the site’s terms.
2. Identify a public sitemap, RSS feed, API, or archive index.
3. Determine whether the site exposes a stable total count or continuation token.
4. Start with one page and inspect the response before scaling up.
5. Use the smallest useful field selection.
6. Keep concurrency low and add backoff.
7. Save source URLs and stable IDs.
8. Extract text without copying media.
9. Write line-oriented records so partial progress is recoverable.
10. Validate JSON, count IDs, detect duplicates, and test decompression.
11. Store a checksum with the output.
12. Keep a report like this one with source, dates, code version, and counts.

## 12. Copyright and responsible use

This dataset contains text from a public news website. Public accessibility does
not automatically grant redistribution rights. Check the source site’s terms,
copyright requirements, and the rules of the jurisdiction where the dataset is
used.

This build intentionally excluded images, video, and audio. For research,
prefer private or access-controlled storage, retain source URLs and attribution,
and do not present the scraped text as original writing.
