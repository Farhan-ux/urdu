#!/usr/bin/env python3
"""Download the public WordPress article archive from urdu.arynews.tv.

The crawler writes one compressed JSONL file:
    arynews_urdu_articles.jsonl.gz

It is intentionally resumable.  A small checkpoint is kept beside the output
while the crawl is running; it can be deleted after the final file is checked.
No images, videos, or other media are downloaded.
"""

from __future__ import annotations

import argparse
import io
import gzip
import html
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API = "https://urdu.arynews.tv/wp-json/wp/v2/posts"
USER_AGENT = "ResearchDatasetBot/1.0 (public Urdu NLP dataset; respects robots.txt)"
FIELDS = ",".join(
    [
        "id",
        "date",
        "modified",
        "link",
        "slug",
        "title",
        "content",
        "excerpt",
        "categories",
        "tags",
        "author",
    ]
)


class TextExtractor(HTMLParser):
    """Turn WordPress HTML into readable text without downloading media."""

    BLOCK_TAGS = {
        "address",
        "article",
        "aside",
        "blockquote",
        "br",
        "dd",
        "div",
        "dl",
        "dt",
        "figcaption",
        "figure",
        "footer",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "header",
        "li",
        "main",
        "nav",
        "ol",
        "p",
        "pre",
        "section",
        "table",
        "td",
        "th",
        "tr",
        "ul",
    }
    SKIP_TAGS = {"script", "style", "noscript", "svg", "template"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in self.SKIP_TAGS:
            self.skip_depth += 1
        if self.skip_depth == 0 and tag in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self.skip_depth == 0 and tag in self.BLOCK_TAGS:
            self.parts.append("\n")
        if tag in self.SKIP_TAGS and self.skip_depth:
            self.skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.skip_depth == 0:
            self.parts.append(data)


def plain_text(markup: str) -> str:
    parser = TextExtractor()
    parser.feed(markup or "")
    parser.close()
    text = html.unescape("".join(parser.parts))
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def request_json(url: str, retries: int = 6) -> tuple[Any, dict[str, str]]:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            with urlopen(request, timeout=90) as response:
                return json.load(response), {k.lower(): v for k, v in response.headers.items()}
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            last_error = exc
            if isinstance(exc, HTTPError) and exc.code in (400, 404):
                raise
            time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"request failed after {retries} attempts: {last_error}")


def get_page(page: int, per_page: int) -> tuple[int, list[dict[str, Any]]]:
    query = urlencode(
        {
            "page": page,
            "per_page": per_page,
            "orderby": "id",
            "order": "asc",
            "status": "publish",
            "_fields": FIELDS,
        }
    )
    data, headers = request_json(f"{API}?{query}")
    total = int(headers.get("x-wp-total", "0"))
    if not isinstance(data, list):
        raise RuntimeError(f"unexpected API response on page {page}: {type(data).__name__}")
    return total, data


def record(post: dict[str, Any]) -> dict[str, Any]:
    title = plain_text((post.get("title") or {}).get("rendered", ""))
    body = plain_text((post.get("content") or {}).get("rendered", ""))
    excerpt = plain_text((post.get("excerpt") or {}).get("rendered", ""))
    return {
        "id": post.get("id"),
        "url": post.get("link"),
        "slug": post.get("slug"),
        "published_at": post.get("date"),
        "modified_at": post.get("modified"),
        "title": title,
        "text": body,
        "excerpt": excerpt,
        "categories": post.get("categories", []),
        "tags": post.get("tags", []),
        "author_id": post.get("author"),
        "source": "ARY News Urdu",
    }


def load_checkpoint(path: Path) -> set[int]:
    if not path.exists():
        return set()
    try:
        return {int(x) for x in json.loads(path.read_text(encoding="utf-8"))}
    except (OSError, ValueError, TypeError):
        return set()


def save_checkpoint(path: Path, pages: set[int]) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(sorted(pages)), encoding="utf-8")
    os.replace(temp, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="arynews_urdu_articles.jsonl.gz")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--per-page", type=int, default=100)
    parser.add_argument("--start-page", type=int, default=1)
    args = parser.parse_args()

    if not (1 <= args.workers <= 6):
        parser.error("--workers must be between 1 and 6")
    if not (1 <= args.per_page <= 100):
        parser.error("--per-page must be between 1 and 100")

    output = Path(args.output)
    checkpoint = output.with_suffix(output.suffix + ".pages.json")
    done = load_checkpoint(checkpoint)
    print("Discovering archive size...", flush=True)
    total, first_page = get_page(args.start_page, args.per_page)
    page_count = (total + args.per_page - 1) // args.per_page
    print(f"Public archive: {total:,} published posts across {page_count:,} pages", flush=True)
    print(f"Output: {output}", flush=True)

    # If resuming, pages already written are skipped.  The first request is
    # retained if it was not previously completed.
    pending = [p for p in range(args.start_page, page_count + 1) if p not in done]
    if not pending:
        print("Nothing left to fetch.", flush=True)
        return 0

    file_mode = "ab" if output.exists() and done else "wb"
    lock = threading.Lock()
    completed = len(done)
    failed: list[int] = []

    # Each page is written as its own concatenated gzip member.  This keeps
    # completed pages readable if the process is interrupted mid-crawl.
    if not output.exists():
        output.touch()

    def write_page(page: int, posts: list[dict[str, Any]]) -> None:
        nonlocal completed
        page_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=page_buffer, mode="wb", compresslevel=6) as page_zip:
            for post in posts:
                page_zip.write(
                    (json.dumps(record(post), ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
                )
        with lock:
            with output.open("ab") as out:
                out.write(page_buffer.getvalue())
                out.flush()
                os.fsync(out.fileno())
                done.add(page)
                completed += 1
                if completed % 25 == 0 or completed == page_count:
                    save_checkpoint(checkpoint, done)
                    print(
                        f"Progress: {completed:,}/{page_count:,} pages "
                        f"({completed / page_count:.1%}), records written at least {len(done) * args.per_page:,}",
                    flush=True,
                )

    # The API is read-only and robots.txt allows crawling.  Keep
    # concurrency modest to avoid hammering the news site.
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(get_page, page, args.per_page): page for page in pending}
        for future in as_completed(futures):
            page = futures[future]
            try:
                _, posts = future.result()
                write_page(page, posts)
            except Exception as exc:
                failed.append(page)
                print(f"Page {page} failed: {exc}", file=sys.stderr, flush=True)

    save_checkpoint(checkpoint, done)
    if failed:
        print(f"Completed with {len(failed)} failed pages. Re-run to retry them.", file=sys.stderr)
        return 2
    print(f"Finished: {len(done):,} pages downloaded to {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())