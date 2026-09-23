#!/usr/bin/env python3
"""
arynews.tv Async Scraper v6
============================
Production-grade scraper using aiohttp + asyncio.

KEY CHANGES FROM v5:
- Replaced requests+ThreadPoolExecutor with aiohttp+asyncio (10x more efficient)
- Single event loop, 100 concurrent connections (vs 16 thread workers)
- Browser-like headers (Cloudflare-friendly)
- Connection keep-alive (no TLS handshake per request)
- CHUNK_ID parameter: split work across multiple Colab sessions
- Adaptive concurrency: auto-reduce if failure rate climbs

USAGE FOR 4-HOUR COMPLETION:
1. Run 4-8 Colab sessions in parallel (different Google accounts)
2. Each session sets CHUNK_ID = 0, 1, 2, ... 7
3. Each session handles 1/8 of URLs at ~6 req/s
4. Combined: 4-8 × 6 = 24-48 req/s → 2-4 hours total

OUTPUT per article:
  url, title, category, published_date, body_text, char_count, scraped_at
"""

import os
import sys
import json
import gzip
import time
import random
import logging
import asyncio
import threading
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse
from collections import Counter

import aiohttp
import lxml.html
import re
from tqdm import tqdm

# ==========================================================================
# CONFIGURATION — EDIT CHUNK_ID BELOW FOR PARALLEL SESSIONS
# ==========================================================================

# === SET THIS FOR EACH PARALLEL COLAB SESSION ===
# Session 1: CHUNK_ID = 0
# Session 2: CHUNK_ID = 1
# ... up to CHUNK_ID = 7 (8 sessions)
# Set to -1 to process ALL URLs in one session (slow, ~15 hours)
CHUNK_ID = 0
TOTAL_CHUNKS = 8  # how many ways to split the work

# === SCRAPER SETTINGS ===
BASE_URL = "https://urdu.arynews.tv"

CONCURRENCY = 80           # concurrent HTTP requests (aiohttp can handle 100+ easily)
REQUEST_TIMEOUT = 15       # seconds
MAX_RETRIES = 2            # fail fast
RETRY_DELAY = 2            # seconds

# Browser-like headers — Cloudflare expects these
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
}

# Adaptive throttling
FAILURE_THRESHOLD = 10         # if 10+ failures in 30s, reduce concurrency
FAILURE_WINDOW = 30
CONCURRENCY_REDUCE_FACTOR = 0.5  # cut concurrency in half when threshold hit
CONCURRENCY_MIN = 10
COOLDOWN_PAUSE = 60            # pause 60s when severely throttled

# Checkpoint
CHECKPOINT_INTERVAL_SECONDS = 60   # save state every minute (frequent!)
PAGES_PER_SHARD = 500

# Output paths (Google Drive)
OUTPUT_DIR = Path('/content/drive/MyDrive/urdu_corpus/arynews')
URLS_DIR = OUTPUT_DIR / 'urls'
ARTICLES_DIR = OUTPUT_DIR / 'articles'
LOGS_DIR = OUTPUT_DIR / 'logs'
CHECKPOINT_DIR = OUTPUT_DIR / 'checkpoint'

# ==========================================================================
# LOGGING
# ==========================================================================

def setup_logging():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    suffix = f"chunk{CHUNK_ID}" if CHUNK_ID >= 0 else "all"
    log_file = LOGS_DIR / f"scrape_{suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    root = logging.getLogger()
    for h in list(root.handlers): root.removeHandler(h)
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s [chunk{CHUNK_ID}] [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger('arynews')

log = logging.getLogger('arynews')

# ==========================================================================
# URL LOADING + CHUNKING
# ==========================================================================

def load_my_chunk():
    """Load all URLs from cache, return only this session's chunk."""
    urls_file = URLS_DIR / "all_urls.jsonl"
    if not urls_file.exists():
        log.error(f"URLs file not found: {urls_file}")
        log.error("Run discovery first (Cell 3 should have created it)")
        sys.exit(1)
    
    all_urls = []
    with open(urls_file, encoding='utf-8') as f:
        for line in f:
            if line.strip():
                all_urls.append(json.loads(line))
    
    if CHUNK_ID < 0:
        log.info(f"CHUNK_ID=-1: processing ALL {len(all_urls):,} URLs")
        return all_urls
    
    # Split into chunks
    chunk_size = len(all_urls) // TOTAL_CHUNKS
    start = CHUNK_ID * chunk_size
    end = (CHUNK_ID + 1) * chunk_size if CHUNK_ID < TOTAL_CHUNKS - 1 else len(all_urls)
    my_chunk = all_urls[start:end]
    
    log.info(f"CHUNK_ID={CHUNK_ID} of {TOTAL_CHUNKS}: URLs [{start:,}:{end:,}] = {len(my_chunk):,} URLs")
    return my_chunk

# ==========================================================================
# ARTICLE EXTRACTION (lxml + XPath, sync function called from async)
# ==========================================================================

_RE_NEWLINES = re.compile(r'\n\s*\n+')
_RE_SPACES = re.compile(r'[ \t]+')

def extract_article(html, url):
    """Extract article data using lxml + XPath. Returns dict or None."""
    if not html:
        return None
    try:
        tree = lxml.html.fromstring(html)
    except Exception:
        return None
    
    # Title
    title_nodes = tree.xpath('//h1[contains(@class, "tdb-title-text")]/text()')
    if not title_nodes:
        title_nodes = tree.xpath('//article//h1//text()')
    if not title_nodes:
        return None
    title = title_nodes[0].strip() if isinstance(title_nodes[0], str) else ''.join(title_nodes).strip()
    if not title:
        return None
    
    # Body
    body_nodes = tree.xpath('//div[contains(@class, "td-post-content")]')
    if not body_nodes:
        return None
    body_elem = body_nodes[0]
    
    for junk_xpath in ['.//style', './/script',
                       './/div[contains(@class, "code-block")]',
                       './/div[contains(@class, "td_block_template")]',
                       './/div[contains(@class, "related")]',
                       './/div[contains(@class, "share")]',
                       './/div[contains(@class, "wp-post-navigation")]',
                       './/div[contains(@class, "td-post-source-tags")]',
                       './/div[contains(@class, "td-post-sharing")]']:
        for junk in body_elem.xpath(junk_xpath):
            if junk.getparent() is not None:
                junk.getparent().remove(junk)
    
    body = body_elem.text_content().strip()
    body = _RE_NEWLINES.sub('\n\n', body)
    body = _RE_SPACES.sub(' ', body)
    if not body or len(body) < 100:
        return None
    
    # Date
    date_nodes = tree.xpath('//time[contains(@class, "entry-date")]/@datetime')
    if not date_nodes:
        date_nodes = tree.xpath('//time/@datetime')
    published = date_nodes[0] if date_nodes else None
    
    # Category
    cat_nodes = tree.xpath('//a[contains(@href, "/category/") and not(contains(@href, "/page/"))]/text()')
    category = 'unknown'
    for c in cat_nodes:
        c = c.strip() if isinstance(c, str) else ''
        if c and len(c) < 30 and c not in ['صفحہ اول', 'ہوم']:
            category = c
            break
    
    return {
        'url': url,
        'title': title,
        'category': category,
        'published_date': published,
        'body_text': body,
        'char_count': len(body),
        'scraped_at': datetime.now(timezone.utc).isoformat(),
    }

# ==========================================================================
# CHECKPOINT / RESUME
# ==========================================================================

def get_checkpoint_file():
    suffix = f"chunk{CHUNK_ID}" if CHUNK_ID >= 0 else "all"
    return CHECKPOINT_DIR / f"progress_{suffix}.json"

def load_checkpoint():
    cp_file = get_checkpoint_file()
    if cp_file.exists():
        with open(cp_file, encoding='utf-8') as f:
            data = json.load(f)
        return {
            'completed_urls': set(data.get('completed_urls', [])),
            'failed_urls': set(data.get('failed_urls', [])),
            'total_saved': data.get('total_saved', 0),
            'current_shard_idx': data.get('current_shard_idx', 0),
            'current_shard_count': data.get('current_shard_count', 0),
            'start_time': data.get('start_time', datetime.now(timezone.utc).isoformat()),
        }
    return {
        'completed_urls': set(),
        'failed_urls': set(),
        'total_saved': 0,
        'current_shard_idx': 0,
        'current_shard_count': 0,
        'start_time': datetime.now(timezone.utc).isoformat(),
    }

def save_checkpoint(state):
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    cp_file = get_checkpoint_file()
    data = {
        'completed_urls': list(state['completed_urls']),
        'failed_urls': list(state['failed_urls']),
        'total_saved': state['total_saved'],
        'current_shard_idx': state['current_shard_idx'],
        'current_shard_count': state['current_shard_count'],
        'start_time': state['start_time'],
        'last_saved_at': datetime.now(timezone.utc).isoformat(),
    }
    tmp = cp_file.with_suffix('.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    tmp.replace(cp_file)

# ==========================================================================
# ASYNC ARTICLE FETCHER
# ==========================================================================

def get_current_shard_path(state):
    suffix = f"chunk{CHUNK_ID}" if CHUNK_ID >= 0 else "all"
    return ARTICLES_DIR / f"articles_{suffix}_{state['current_shard_idx']:04d}.jsonl.gz"

# Synchronous file write (called from async via executor)
_file_lock = threading.Lock()

def append_to_shard_sync(state, article):
    with _file_lock:
        ARTICLES_DIR.mkdir(parents=True, exist_ok=True)
        shard_path = get_current_shard_path(state)
        with gzip.open(shard_path, 'at', encoding='utf-8') as f:
            f.write(json.dumps(article, ensure_ascii=False) + '\n')
        state['current_shard_count'] += 1
        state['total_saved'] += 1
        if state['current_shard_count'] >= PAGES_PER_SHARD:
            state['current_shard_idx'] += 1
            state['current_shard_count'] = 0

async def fetch_one(session, url, sem):
    """Fetch and extract one article. Returns (url, article_dict_or_None)."""
    async with sem:
        for attempt in range(MAX_RETRIES + 1):
            try:
                async with session.get(url, headers=HEADERS,
                                       timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as r:
                    if r.status == 200:
                        html = await r.text()
                        # Run extraction in thread pool (lxml is sync)
                        article = await asyncio.get_event_loop().run_in_executor(
                            None, extract_article, html, url
                        )
                        return url, article
                    elif r.status in (429, 503):
                        await asyncio.sleep(RETRY_DELAY * 2)
                        continue
                    else:
                        return url, None  # 404, etc - don't retry
            except (asyncio.TimeoutError, aiohttp.ClientError):
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    return url, None
            except Exception as e:
                return url, None
    return url, None

async def fetch_all_articles(url_records):
    """Main async fetch loop with adaptive concurrency + checkpointing."""
    state = load_checkpoint()
    log.info(f"Resuming: {len(state['completed_urls']):,} done, "
             f"{len(state['failed_urls']):,} previously failed")
    
    pending = [r for r in url_records if r['url'] not in state['completed_urls']]
    log.info(f"Pending: {len(pending):,} articles")
    
    if not pending:
        log.info("Nothing to do — all articles in this chunk already scraped.")
        return state
    
    # Adaptive concurrency state
    current_concurrency = CONCURRENCY
    sem = asyncio.Semaphore(current_concurrency)
    failure_timestamps = []
    last_checkpoint = time.time()
    fetch_start = time.time()
    
    # Connector with keep-alive
    connector = aiohttp.TCPConnector(
        limit=current_concurrency + 20,
        limit_per_host=current_concurrency,
        keepalive_timeout=30,
        enable_cleanup_closed=True,
    )
    
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    # Progress bar
    pbar = tqdm(total=len(pending), desc=f"Chunk{CHUNK_ID}", unit='art')
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Process in batches to enable adaptive concurrency
        BATCH = 200
        for batch_start in range(0, len(pending), BATCH):
            batch = pending[batch_start:batch_start + BATCH]
            tasks = [fetch_one(session, r['url'], sem) for r in batch]
            
            for coro in asyncio.as_completed(tasks):
                url, article = await coro
                
                if article is not None:
                    await asyncio.get_event_loop().run_in_executor(
                        None, append_to_shard_sync, state, article
                    )
                    state['completed_urls'].add(url)
                else:
                    state['failed_urls'].add(url)
                    failure_timestamps.append(time.time())
                
                pbar.update(1)
                pbar.set_postfix(
                    saved=state['total_saved'],
                    failed=len(state['failed_urls']),
                    conc=current_concurrency
                )
            
            # Clean old failure timestamps
            cutoff = time.time() - FAILURE_WINDOW
            failure_timestamps[:] = [t for t in failure_timestamps if t > cutoff]
            
            # Adaptive concurrency: too many failures → reduce
            if len(failure_timestamps) >= FAILURE_THRESHOLD:
                new_concurrency = max(CONCURRENCY_MIN, int(current_concurrency * CONCURRENCY_REDUCE_FACTOR))
                if new_concurrency != current_concurrency:
                    log.warning(f"[ADAPTIVE] {len(failure_timestamps)} failures in {FAILURE_WINDOW}s — "
                                f"reducing concurrency {current_concurrency} → {new_concurrency}")
                    current_concurrency = new_concurrency
                    sem = asyncio.Semaphore(current_concurrency)
                    failure_timestamps.clear()
                    # Brief pause to let any pending requests finish
                    await asyncio.sleep(2)
            
            # Checkpoint
            if time.time() - last_checkpoint > CHECKPOINT_INTERVAL_SECONDS:
                await asyncio.get_event_loop().run_in_executor(None, save_checkpoint, state)
                last_checkpoint = time.time()
    
    pbar.close()
    
    # Final checkpoint
    save_checkpoint(state)
    
    elapsed = time.time() - fetch_start
    rate = state['total_saved'] / max(elapsed, 1)
    log.info(f"DONE. {state['total_saved']:,} saved, {len(state['failed_urls']):,} failed")
    log.info(f"Elapsed: {elapsed:.0f}s, Rate: {rate:.1f} art/s")
    return state

# ==========================================================================
# SUMMARY
# ==========================================================================

def print_summary(state):
    log.info("=" * 60)
    log.info(f"CHUNK {CHUNK_ID} SUMMARY")
    log.info("=" * 60)
    log.info(f"Total saved:  {state['total_saved']:,}")
    log.info(f"Failed URLs:  {len(state['failed_urls']):,}")
    log.info(f"Started:      {state['start_time']}")
    log.info("")
    log.info("Shard files:")
    suffix = f"chunk{CHUNK_ID}" if CHUNK_ID >= 0 else "all"
    for shard in sorted(ARTICLES_DIR.glob(f"articles_{suffix}_*.jsonl.gz")):
        size_mb = shard.stat().st_size / 1024 / 1024
        log.info(f"  {shard.name}: {size_mb:.2f} MB")

# ==========================================================================
# MAIN
# ==========================================================================

def main():
    for d in [OUTPUT_DIR, URLS_DIR, ARTICLES_DIR, LOGS_DIR, CHECKPOINT_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    setup_logging()
    
    log.info("=" * 60)
    log.info(f"ARYNEWS ASYNC SCRAPER - CHUNK {CHUNK_ID}/{TOTAL_CHUNKS}")
    log.info("=" * 60)
    log.info(f"Concurrency: {CONCURRENCY}")
    log.info(f"Timeout: {REQUEST_TIMEOUT}s, Retries: {MAX_RETRIES}")
    log.info(f"Checkpoint every: {CHECKPOINT_INTERVAL_SECONDS}s")
    log.info("=" * 60)
    
    # Load URLs for this chunk
    url_records = load_my_chunk()
    
    if not url_records:
        log.error("No URLs to process!")
        return
    
    # Run async scraper
    state = asyncio.run(fetch_all_articles(url_records))
    
    print_summary(state)

if __name__ == "__main__":
    main()
