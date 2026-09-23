#!/usr/bin/env python3
"""
arynews.tv Urdu News Scraper
=============================
- Discovers article URLs by walking category pagination
- Fetches articles concurrently with retries
- Saves to gzip JSONL with checkpoint/resume
- Designed for Google Colab (12h session, Drive persistence)

Output fields per article:
  url, title, category, published_date, body_text, char_count, scraped_at
"""

import os
import sys
import json
import gzip
import time
import random
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ==========================================================================
# CONFIGURATION
# ==========================================================================

BASE_URL = "https://urdu.arynews.tv"

# 10 categories (multimedia excluded - it's video/photo galleries only)
CATEGORIES = {
    "pakistan-2":         "پاکستان",
    "international-2":    "عالمی خبریں",
    "sports-2":           "کھیل",
    "unsual":             "حیرت انگیز",
    "کاروباری-خبریں":     "تجارت",
    "sehat":              "صحت",
    "fun-o-sakafat":      "فن و ثقافت",
    "miscellaneous":      "میگزین",
    "سائنس-اور-ٹیکنالوجی": "سائنس اور ٹیکنالوجی",
    "urdu-blogs":         "بلاگز",
}

# Network settings
WORKERS = 16                   # concurrent threads for article fetching
PAGE_WORKERS = 8               # concurrent threads for URL discovery
REQUEST_TIMEOUT = 20           # seconds
MAX_RETRIES = 4
RETRY_BACKOFF = [2, 5, 15, 30]  # seconds between retries
RATE_LIMIT_DELAY = (0.05, 0.15)  # random sleep between requests (politeness)

# User agents (rotate to avoid simple UA-based blocking)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# Checkpoint settings
CHECKPOINT_INTERVAL_SECONDS = 5 * 60    # save state at least every 5 minutes
CHECKPOINT_INTERVAL_ARTICLES = 500      # ...or every 500 articles, whichever first
MAX_PAGES_PER_CATEGORY = 5000           # safety cap (Pakistan has ~5000+ pages)
PAGES_PER_SHARD = 500                   # how many articles per JSONL shard file
MONITOR_INTERVAL_SECONDS = 15           # how often to print live status

# Output paths (overridden in Colab to point to Drive)
OUTPUT_DIR = Path("./urdu_corpus/arynews")
URLS_DIR = OUTPUT_DIR / "urls"
ARTICLES_DIR = OUTPUT_DIR / "articles"
LOGS_DIR = OUTPUT_DIR / "logs"
CHECKPOINT_DIR = OUTPUT_DIR / "checkpoint"

# ==========================================================================
# LOGGING
# ==========================================================================

def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / f"scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger("arynews")

log = logging.getLogger("arynews")

# ==========================================================================
# HTTP CLIENT
# ==========================================================================

class HttpClient:
    """Thread-safe HTTP client with retries and UA rotation."""

    def __init__(self):
        self.session = requests.Session()
        self._lock = threading.Lock()

    def get(self, url):
        for attempt in range(MAX_RETRIES):
            try:
                headers = {"User-Agent": random.choice(USER_AGENTS)}
                # Polite delay
                time.sleep(random.uniform(*RATE_LIMIT_DELAY))
                r = self.session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    return r.text
                elif r.status_code in (429, 503):
                    wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)] * 2
                    log.warning(f"HTTP {r.status_code} on {url}, backing off {wait}s")
                    time.sleep(wait)
                elif r.status_code == 404:
                    return None  # page doesn't exist (end of pagination)
                else:
                    log.warning(f"HTTP {r.status_code} on {url}")
                    time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)])
            except requests.RequestException as e:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                log.warning(f"Attempt {attempt+1} failed for {url}: {type(e).__name__}")
                time.sleep(wait)
        log.error(f"Giving up on {url} after {MAX_RETRIES} attempts")
        return None

http = HttpClient()

# ==========================================================================
# URL DISCOVERY
# ==========================================================================

def extract_article_urls_from_category_page(html):
    """Extract article URLs from a category index page.
    arynews uses TagDiv theme; article cards live in <h3 class='entry-title'><a href>.
    """
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    urls = []
    for h3 in soup.find_all("h3", class_="entry-title"):
        a = h3.find("a", href=True)
        if a:
            href = a["href"]
            # Validate: must be article URL, not category/page/tag
            if (href.startswith(BASE_URL + "/")
                and "/category/" not in href
                and "/page/" not in href
                and "/tag/" not in href
                and "/author/" not in href
                and "?s=" not in href
                and "wp-" not in href):
                # Must be a single-segment slug after domain
                path = urlparse(href).path.strip("/")
                if "/" not in path and len(path) > 5:
                    urls.append(href)
    # Dedupe preserving order
    seen = set()
    unique = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    return unique


def discover_urls_for_category(category_slug, category_name):
    """Walk pagination for one category, return set of article URLs."""
    log.info(f"[DISCOVERY] Starting category: {category_name} ({category_slug})")
    discovered = []
    seen_urls = set()

    # Build page URLs
    page_urls = []
    for page_num in range(1, MAX_PAGES_PER_CATEGORY + 1):
        if page_num == 1:
            page_urls.append((1, f"{BASE_URL}/category/{category_slug}/"))
        else:
            page_urls.append((page_num, f"{BASE_URL}/category/{category_slug}/page/{page_num}/"))

    # Process pages in batches (concurrent within batch, sequential across batches)
    BATCH_SIZE = 20
    empty_streak = 0
    last_seen_count = 0

    with tqdm(total=min(MAX_PAGES_PER_CATEGORY, len(page_urls)),
              desc=f"  {category_name}", unit="pg") as pbar:
        for batch_start in range(0, len(page_urls), BATCH_SIZE):
            batch = page_urls[batch_start:batch_start + BATCH_SIZE]

            with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as ex:
                futures = {ex.submit(http.get, url): (pnum, url) for pnum, url in batch}
                results = {}
                for fut in as_completed(futures):
                    pnum, url = futures[fut]
                    html = fut.result()
                    results[pnum] = html

            # Process in order
            batch_new_urls = 0
            for pnum, url in batch:
                html = results.get(pnum)
                if html is None:
                    empty_streak += 1
                    continue
                page_urls_found = extract_article_urls_from_category_page(html)
                if not page_urls_found:
                    empty_streak += 1
                    continue

                # Check if all URLs on this page are already seen (archive end)
                new_on_page = [u for u in page_urls_found if u not in seen_urls]
                if not new_on_page:
                    empty_streak += 1
                else:
                    empty_streak = 0
                    for u in new_on_page:
                        seen_urls.add(u)
                        discovered.append((u, category_slug, category_name))
                        batch_new_urls += len(new_on_page)

                pbar.update(1)

            # If we got 5 consecutive empty/dup pages, archive is exhausted
            if empty_streak >= 5:
                log.info(f"  [DISCOVERY] {category_name}: stopped at page {batch_start + len(batch)} (archive end)")
                break

            # Don't continue past last batch
            if batch_start + BATCH_SIZE >= len(page_urls):
                break

    log.info(f"[DISCOVERY] {category_name}: found {len(discovered)} article URLs")
    return discovered


def discover_all_urls():
    """Run URL discovery for all categories, save to file."""
    URLS_DIR.mkdir(parents=True, exist_ok=True)
    all_urls = []
    for cat_slug, cat_name in CATEGORIES.items():
        urls_file = URLS_DIR / f"urls_{cat_slug}.jsonl"
        if urls_file.exists():
            log.info(f"[DISCOVERY] {cat_name}: URLs file exists, loading")
            with open(urls_file, encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        all_urls.append(json.loads(line))
            continue

        urls = discover_urls_for_category(cat_slug, cat_name)
        with open(urls_file, "w", encoding="utf-8") as f:
            for u, cs, cn in urls:
                f.write(json.dumps({"url": u, "category_slug": cs, "category_name": cn}, ensure_ascii=False) + "\n")
        for u, cs, cn in urls:
            all_urls.append({"url": u, "category_slug": cs, "category_name": cn})

    log.info(f"[DISCOVERY] TOTAL: {len(all_urls)} article URLs across all categories")
    return all_urls

# ==========================================================================
# ARTICLE EXTRACTION
# ==========================================================================

def extract_article(html, url, category_name):
    """Extract structured data from an article page."""
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")

    # Title: <h1 class="tdb-title-text"> is the actual article title
    # (NOT entry-title which is for sidebar/related links)
    title = None
    h1 = soup.find("h1", class_="tdb-title-text")
    if h1:
        title = h1.get_text(strip=True)
    else:
        # Fallback: first h1 inside <article>
        article_tag = soup.find("article")
        if article_tag:
            h1 = article_tag.find("h1")
            if h1:
                title = h1.get_text(strip=True)
    if not title:
        return None  # probably not an article

    # Body: <div class="td-post-content">
    body = None
    body_div = soup.find("div", class_="td-post-content")
    if body_div:
        # Remove inline ads, related boxes
        for junk in body_div.find_all(class_=["code-block", "td_block_template", "related", "share", "wp-post-navigation"]):
            junk.decompose()
        body = body_div.get_text("\n", strip=True)
    if not body or len(body) < 100:
        # Try alternate containers
        for cls in ["tdb-block-inner", "entry-content"]:
            alt = soup.find(class_=cls)
            if alt:
                txt = alt.get_text("\n", strip=True)
                if txt and len(txt) > len(body or ""):
                    body = txt
    if not body or len(body) < 100:
        return None  # too short, probably a stub or video page

    # Date: <time class="entry-date" datetime="...">
    published = None
    time_tag = soup.find("time", class_="entry-date")
    if time_tag and time_tag.get("datetime"):
        published = time_tag["datetime"]
    else:
        time_tag = soup.find("time")
        if time_tag and time_tag.get("datetime"):
            published = time_tag["datetime"]

    return {
        "url": url,
        "title": title,
        "category": category_name,
        "published_date": published,
        "body_text": body,
        "char_count": len(body),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_one_article(url, category_name):
    """Fetch and extract one article. Returns dict or None."""
    html = http.get(url)
    if html is None:
        return None
    return extract_article(html, url, category_name)

# ==========================================================================
# CHECKPOINT / RESUME
# ==========================================================================

def get_checkpoint_file():
    """Lazy resolution — allows OUTPUT_DIR override after import."""
    return CHECKPOINT_DIR / "progress.json"

def load_checkpoint():
    """Returns dict with completed_urls set, current_shard_idx, total_saved."""
    checkpoint_file = get_checkpoint_file()
    if checkpoint_file.exists():
        with open(checkpoint_file, encoding="utf-8") as f:
            data = json.load(f)
        return {
            "completed_urls": set(data.get("completed_urls", [])),
            "failed_urls": set(data.get("failed_urls", [])),
            "total_saved": data.get("total_saved", 0),
            "current_shard_idx": data.get("current_shard_idx", 0),
            "current_shard_count": data.get("current_shard_count", 0),
            "start_time": data.get("start_time", datetime.now(timezone.utc).isoformat()),
        }
    return {
        "completed_urls": set(),
        "failed_urls": set(),
        "total_saved": 0,
        "current_shard_idx": 0,
        "current_shard_count": 0,
        "start_time": datetime.now(timezone.utc).isoformat(),
    }

def save_checkpoint(state):
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_file = get_checkpoint_file()
    data = {
        "completed_urls": list(state["completed_urls"]),
        "failed_urls": list(state["failed_urls"]),
        "total_saved": state["total_saved"],
        "current_shard_idx": state["current_shard_idx"],
        "current_shard_count": state["current_shard_count"],
        "start_time": state["start_time"],
        "last_saved_at": datetime.now(timezone.utc).isoformat(),
    }
    tmp = checkpoint_file.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    tmp.replace(checkpoint_file)  # atomic

# ==========================================================================
# ARTICLE FETCH LOOP (with checkpointing + live status monitor)
# ==========================================================================

def get_current_shard_path(state):
    return ARTICLES_DIR / f"articles_{state['current_shard_idx']:04d}.jsonl.gz"

def append_to_shard(state, article):
    """Append one article to the current gzip JSONL shard.
    If shard is full, roll to next.
    """
    ARTICLES_DIR.mkdir(parents=True, exist_ok=True)
    shard_path = get_current_shard_path(state)
    with gzip.open(shard_path, "at", encoding="utf-8") as f:
        f.write(json.dumps(article, ensure_ascii=False) + "\n")
    state["current_shard_count"] += 1
    state["total_saved"] += 1
    if state["current_shard_count"] >= PAGES_PER_SHARD:
        state["current_shard_idx"] += 1
        state["current_shard_count"] = 0

def format_elapsed(seconds):
    """Format seconds as HH:MM:SS."""
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def read_last_checkpoint_time():
    """Read last_saved_at from checkpoint file, return as datetime or None."""
    cp_file = get_checkpoint_file()
    if not cp_file.exists():
        return None
    try:
        with open(cp_file, encoding="utf-8") as f:
            data = json.load(f)
        last = data.get("last_saved_at")
        if last:
            return datetime.fromisoformat(last.replace("Z", "+00:00"))
    except Exception:
        pass
    return None

def stats_monitor(state, fetch_start_time, total_pending, stop_event):
    """Background thread that prints live stats while scraping.
    Shows: elapsed time, articles saved, rate, last checkpoint, ETA.
    """
    while not stop_event.is_set():
        elapsed = time.time() - fetch_start_time
        saved = state["total_saved"]
        failed = len(state["failed_urls"])
        completed = len(state["completed_urls"])
        rate = saved / max(elapsed, 1)

        # Time since last checkpoint (i.e. "data safe up to X ago")
        last_cp = read_last_checkpoint_time()
        if last_cp:
            since_cp = (datetime.now(timezone.utc) - last_cp).total_seconds()
            cp_str = f"{format_elapsed(since_cp)} ago"
        else:
            cp_str = "not yet"

        # ETA
        remaining = total_pending - completed
        eta_str = format_elapsed(remaining / rate) if rate > 0.1 else "??"

        # Build status line
        line = (
            f"\r[{format_elapsed(elapsed)}] "
            f"Saved: {saved:,} | "
            f"Failed: {failed:,} | "
            f"Rate: {rate:.1f}/s | "
            f"Last Drive save: {cp_str} | "
            f"ETA: {eta_str}   "
        )
        sys.stdout.write(line)
        sys.stdout.flush()

        stop_event.wait(MONITOR_INTERVAL_SECONDS)

    # Final status when scraping ends
    elapsed = time.time() - fetch_start_time
    saved = state["total_saved"]
    failed = len(state["failed_urls"])
    rate = saved / max(elapsed, 1)
    sys.stdout.write("\r" + " " * 100 + "\r")  # clear line
    sys.stdout.flush()
    log.info(
        f"[FINAL] {format_elapsed(elapsed)} elapsed | "
        f"{saved:,} saved | {failed:,} failed | "
        f"{rate:.1f} articles/sec avg"
    )

def fetch_all_articles(url_records):
    """Main fetch loop with checkpointing + live monitor."""
    state = load_checkpoint()
    log.info(f"[FETCH] Resuming: {len(state['completed_urls'])} already done, "
             f"{len(url_records) - len(state['completed_urls'])} remaining")

    # Filter out completed
    pending = [r for r in url_records if r["url"] not in state["completed_urls"]]
    log.info(f"[FETCH] Pending: {len(pending)} articles")

    if not pending:
        log.info("[FETCH] Nothing to do — all articles already scraped.")
        return state

    last_checkpoint = time.time()
    articles_since_checkpoint = 0
    lock = threading.Lock()

    # Start live monitor in background
    fetch_start_time = time.time()
    stop_event = threading.Event()
    monitor_thread = threading.Thread(
        target=stats_monitor,
        args=(state, fetch_start_time, len(pending), stop_event),
        daemon=True,
    )
    monitor_thread.start()

    with tqdm(total=len(pending), desc="Articles", unit="art") as pbar:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futures = {ex.submit(fetch_one_article, r["url"], r["category_name"]): r for r in pending}

            for fut in as_completed(futures):
                r = futures[fut]
                url = r["url"]
                try:
                    article = fut.result()
                except Exception as e:
                    log.error(f"Exception on {url}: {type(e).__name__}: {e}")
                    article = None

                with lock:
                    if article is not None:
                        append_to_shard(state, article)
                        state["completed_urls"].add(url)
                    else:
                        state["failed_urls"].add(url)

                    # Checkpoint by time OR by article count
                    now = time.time()
                    articles_since_checkpoint += 1
                    if (now - last_checkpoint > CHECKPOINT_INTERVAL_SECONDS
                        or articles_since_checkpoint >= CHECKPOINT_INTERVAL_ARTICLES):
                        save_checkpoint(state)
                        last_checkpoint = now
                        articles_since_checkpoint = 0
                        log.info(
                            f"[CHECKPOINT] Saved to Drive. "
                            f"Total: {state['total_saved']:,} articles, "
                            f"{len(state['failed_urls']):,} failed"
                        )

                pbar.update(1)
                pbar.set_postfix(saved=state["total_saved"], failed=len(state["failed_urls"]))

    # Stop monitor
    stop_event.set()
    monitor_thread.join(timeout=5)

    # Final checkpoint
    save_checkpoint(state)
    log.info(f"[FETCH] DONE. Total saved: {state['total_saved']:,}, "
             f"failed: {len(state['failed_urls']):,}")
    return state

# ==========================================================================
# STATS / SUMMARY
# ==========================================================================

def print_summary(state):
    log.info("=" * 60)
    log.info("SCRAPING SUMMARY")
    log.info("=" * 60)
    log.info(f"Total articles saved:  {state['total_saved']}")
    log.info(f"Failed URLs:           {len(state['failed_urls'])}")
    log.info(f"Started at:            {state['start_time']}")
    log.info(f"Last checkpoint:       {datetime.now(timezone.utc).isoformat()}")
    log.info("")
    log.info("Per-shard file sizes:")
    for shard in sorted(ARTICLES_DIR.glob("articles_*.jsonl.gz")):
        size_mb = shard.stat().st_size / 1024 / 1024
        log.info(f"  {shard.name}: {size_mb:.2f} MB")
    log.info("")
    log.info(f"Output directory: {OUTPUT_DIR}")
    log.info("=" * 60)

# ==========================================================================
# MAIN
# ==========================================================================

def main():
    # Setup
    for d in [OUTPUT_DIR, URLS_DIR, ARTICLES_DIR, LOGS_DIR, CHECKPOINT_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    setup_logging()

    log.info("=" * 60)
    log.info("ARYNEWS.tv URDU SCRAPER")
    log.info("=" * 60)
    log.info(f"Categories: {len(CATEGORIES)}")
    log.info(f"Workers: {WORKERS} (articles), {PAGE_WORKERS} (discovery)")
    log.info(f"Checkpoint interval: {CHECKPOINT_INTERVAL_SECONDS}s")
    log.info(f"Output: {OUTPUT_DIR}")
    log.info("=" * 60)

    # Phase 1: URL discovery
    log.info("\n>>> PHASE 1: URL DISCOVERY <<<")
    url_records = discover_all_urls()

    # Phase 2: Article fetching
    log.info("\n>>> PHASE 2: ARTICLE FETCHING <<<")
    state = fetch_all_articles(url_records)

    # Summary
    print_summary(state)

if __name__ == "__main__":
    main()
