"""
YouTube channel metadata scraper using Playwright.

Navigates to a channel page + its /about tab, extracts ytInitialData from
the embedded JavaScript variable, and returns a normalised dict.  Every
optional field (views, joined_date, social links, etc.) silently falls back
to None rather than raising — Airflow will receive the full dict regardless.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class YouTubeScraperError(Exception):
    """Raised when scraping fails unrecoverably."""


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _safe_get(obj: Any, *keys: Any, default: Any = None) -> Any:
    """Traverse a nested dict/list without raising."""
    for key in keys:
        try:
            obj = obj[key]
        except (KeyError, IndexError, TypeError):
            return default
    return obj


def _deep_search(obj: Any, target_key: str, _depth: int = 0) -> Any:
    """Return the first value found for *target_key* in a nested structure."""
    if _depth > 40:
        return None
    if isinstance(obj, dict):
        if target_key in obj:
            return obj[target_key]
        for v in obj.values():
            result = _deep_search(v, target_key, _depth + 1)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _deep_search(item, target_key, _depth + 1)
            if result is not None:
                return result
    return None


def _extract_text(obj: Any) -> Optional[str]:
    """Return a plain string from the various YouTube text node formats."""
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj.strip() or None
    if isinstance(obj, dict):
        if "simpleText" in obj:
            return obj["simpleText"].strip() or None
        if "runs" in obj:
            return "".join(r.get("text", "") for r in obj["runs"]).strip() or None
        if "content" in obj:
            return obj["content"].strip() or None
        # accessibility fallback
        label = _safe_get(obj, "accessibility", "accessibilityData", "label")
        if label:
            return str(label).strip() or None
    return None


# ---------------------------------------------------------------------------
# ytInitialData extraction
# ---------------------------------------------------------------------------

async def _get_yt_initial_data(page: Any) -> dict:
    """
    Try to read window.ytInitialData via JS evaluation.
    Falls back to regex extraction from the raw HTML if evaluation fails.
    """
    try:
        data = await page.evaluate("() => window.ytInitialData || null")
        if data and isinstance(data, dict):
            logger.debug("ytInitialData extracted via JS evaluation")
            return data
    except Exception as exc:
        logger.warning("JS evaluation failed (%s) — trying HTML fallback", exc)

    try:
        html = await page.content()
        match = re.search(
            r'(?:var\s+ytInitialData|window\["ytInitialData"\])\s*=\s*(\{.*?\});\s*</script>',
            html,
            re.DOTALL,
        )
        if match:
            logger.debug("ytInitialData extracted via HTML regex")
            return json.loads(match.group(1))
    except Exception as exc:
        logger.warning("HTML regex extraction failed: %s", exc)

    return {}


# ---------------------------------------------------------------------------
# Header parsing  (supports both legacy and newer YouTube layouts)
# ---------------------------------------------------------------------------

def _extract_header(yt_data: dict) -> dict:
    """
    Parse channel identity fields from the page header object.

    YouTube currently ships at least two header formats:
      - c4TabbedHeaderRenderer  (legacy / still common)
      - pageHeaderRenderer      (newer "Polymer" design)
    Both are attempted; the first non-None value wins.
    """
    out: dict[str, Optional[str]] = {
        "name": None,
        "username": None,
        "picture_url": None,
        "subscribers": None,
        "videos": None,
    }

    header = yt_data.get("header", {})

    # ── c4TabbedHeaderRenderer ──────────────────────────────────────────────
    c4 = header.get("c4TabbedHeaderRenderer", {})
    if c4:
        out["name"] = _extract_text(c4.get("title"))
        out["username"] = _extract_text(c4.get("channelHandleText"))

        thumbnails: list = _safe_get(c4, "avatar", "thumbnails") or []
        if thumbnails:
            out["picture_url"] = thumbnails[-1].get("url")

        out["subscribers"] = _extract_text(c4.get("subscriberCountText"))
        out["videos"] = _extract_text(c4.get("videosCountText"))

    # ── pageHeaderRenderer  ─────────────────────────────────────────────────
    phv = _safe_get(header, "pageHeaderRenderer", "content", "pageHeaderViewModel")
    if phv:
        if not out["name"]:
            out["name"] = _safe_get(phv, "title", "dynamicTextViewModel", "text", "content")

        if not out["picture_url"]:
            sources: list = (
                _safe_get(
                    phv,
                    "image",
                    "decoratedAvatarViewModel",
                    "avatar",
                    "avatarViewModel",
                    "image",
                    "sources",
                )
                or []
            )
            if sources:
                out["picture_url"] = sources[-1].get("url")

        # Metadata rows contain handle / subscriber / video counts
        rows: list = (
            _safe_get(phv, "metadata", "contentMetadataViewModel", "metadataRows") or []
        )
        for row in rows:
            for part in row.get("metadataParts", []):
                text: str = _safe_get(part, "text", "content") or ""
                if text.startswith("@") and not out["username"]:
                    out["username"] = text
                elif "subscriber" in text.lower() and not out["subscribers"]:
                    out["subscribers"] = text
                elif "video" in text.lower() and not out["videos"]:
                    out["videos"] = text

    return out


# ---------------------------------------------------------------------------
# About-page parsing
# ---------------------------------------------------------------------------

def _parse_external_link(link_obj: dict) -> tuple[Optional[str], Optional[str]]:
    """
    Return (title, url) from a channelExternalLinkViewModel dict.
    YouTube stores the navigable URL in several possible sub-paths.
    """
    title = _extract_text(link_obj.get("title"))
    link_node = link_obj.get("link") or {}

    # Prefer explicit content field
    url: Optional[str] = link_node.get("content")

    # Fallback: navigate through commandRuns
    if not url:
        url = _safe_get(
            link_node,
            "commandRuns",
            0,
            "onTap",
            "innertubeCommand",
            "urlEndpoint",
            "url",
        )

    # Fallback: innertubeCommand directly on the link
    if not url:
        url = _safe_get(link_node, "innertubeCommand", "urlEndpoint", "url")

    return title, url


def _extract_about(yt_data: dict) -> dict:
    """
    Parse description, stats, and social links from the About section.

    The aboutChannelViewModel can appear in several locations depending on
    whether the user navigated to the /about tab or the data is embedded in
    an engagementPanel on the main page.
    """
    out: dict[str, Any] = {
        "description": None,
        "views": None,
        "joined_date": None,
        "website": None,
        "facebook": None,
        "instagram": None,
        "twitter": None,
        "tiktok": None,
        "community": None,
    }

    about_vm = _deep_search(yt_data, "aboutChannelViewModel")
    if about_vm:
        out["description"] = _extract_text(about_vm.get("description"))
        out["views"] = _extract_text(about_vm.get("viewCountText"))
        out["joined_date"] = _extract_text(about_vm.get("joinedDateText"))

        for link in (about_vm.get("links") or []):
            cel = link.get("channelExternalLinkViewModel")
            if not cel:
                continue
            title, url = _parse_external_link(cel)
            if not url:
                continue
            url_lower = url.lower()
            title_lower = (title or "").lower()

            if "facebook.com" in url_lower or "facebook" in title_lower:
                out["facebook"] = out["facebook"] or url
            elif "instagram.com" in url_lower or "instagram" in title_lower:
                out["instagram"] = out["instagram"] or url
            elif "twitter.com" in url_lower or "x.com" in url_lower or "twitter" in title_lower:
                out["twitter"] = out["twitter"] or url
            elif "tiktok.com" in url_lower or "tiktok" in title_lower:
                out["tiktok"] = out["tiktok"] or url
            else:
                out["website"] = out["website"] or url

    # Fallback description from channelMetadataRenderer
    if not out["description"]:
        meta = _safe_get(yt_data, "metadata", "channelMetadataRenderer")
        if meta:
            out["description"] = meta.get("description") or None

    # Check for a Community tab
    tabs: list = (
        _safe_get(yt_data, "contents", "twoColumnBrowseResultsRenderer", "tabs") or []
    )
    for tab in tabs:
        tr = tab.get("tabRenderer", {})
        if tr.get("title") == "Community":
            community_path = _safe_get(
                tr, "endpoint", "commandMetadata", "webCommandMetadata", "url"
            )
            if community_path:
                out["community"] = f"https://www.youtube.com{community_path}"
            break

    return out


# ---------------------------------------------------------------------------
# Main async scraper
# ---------------------------------------------------------------------------

async def _scrape_async(channel_url: str) -> dict:
    """
    Async implementation of the YouTube channel scraper.

    Args:
        channel_url: Full YouTube channel URL.

    Returns:
        Normalised channel metadata dict.

    Raises:
        YouTubeScraperError: On unrecoverable failures.
    """
    from playwright.async_api import async_playwright  # local import keeps module importable without playwright installed

    t0 = time.monotonic()
    logger.info("Launching Playwright Chromium browser")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        logger.info("Browser launched (%.2fs)", time.monotonic() - t0)

        try:
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                timezone_id="America/New_York",
                viewport={"width": 1280, "height": 900},
            )
            page = await ctx.new_page()

            # ── Step 1: Main channel page ───────────────────────────────────
            logger.info("Navigating to channel: %s", channel_url)
            response = await page.goto(
                channel_url, wait_until="domcontentloaded", timeout=60_000
            )

            if response is None or not response.ok:
                status = response.status if response else "no response"
                raise YouTubeScraperError(
                    f"Channel page returned HTTP {status}: {channel_url}"
                )
            logger.info("Channel page loaded (HTTP %s)", response.status)

            # Wait for ytInitialData to be populated
            try:
                await page.wait_for_function(
                    "() => typeof window.ytInitialData !== 'undefined' && window.ytInitialData !== null",
                    timeout=30_000,
                )
            except Exception:
                logger.warning("Timed out waiting for ytInitialData — continuing anyway")

            # Dismiss cookie / consent dialog if present
            try:
                accept = page.get_by_role(
                    "button",
                    name=re.compile(r"Accept all|I agree|Accept", re.IGNORECASE),
                )
                if await accept.count() > 0:
                    await accept.first.click()
                    await page.wait_for_load_state("domcontentloaded", timeout=10_000)
                    logger.info("Dismissed cookie consent dialog")
            except Exception:
                pass

            logger.info("Extracting ytInitialData from main page")
            main_data = await _get_yt_initial_data(page)
            if not main_data:
                raise YouTubeScraperError(
                    f"ytInitialData is empty for {channel_url} — page may not have loaded"
                )

            logger.info("Parsing channel header")
            header = _extract_header(main_data)

            # ── Step 2: About tab ───────────────────────────────────────────
            about_url = channel_url.rstrip("/") + "/about"
            logger.info("Navigating to About tab: %s", about_url)
            about_data: dict = {}
            try:
                await page.goto(about_url, wait_until="domcontentloaded", timeout=60_000)
                await page.wait_for_function(
                    "() => typeof window.ytInitialData !== 'undefined'",
                    timeout=20_000,
                )
                about_raw = await _get_yt_initial_data(page)
                logger.info("Parsing About data")
                about_data = _extract_about(about_raw)
            except Exception as exc:
                logger.warning(
                    "About page scrape failed (%s) — falling back to main page data", exc
                )
                about_data = _extract_about(main_data)

            # ── Fallback: extract username from URL ─────────────────────────
            if not header.get("username"):
                m = re.search(
                    r"youtube\.com/(@[\w.\-]+|c/[\w.\-]+|channel/[\w.\-]+|user/[\w.\-]+)",
                    channel_url,
                )
                if m:
                    header["username"] = m.group(1)
                    logger.info("Username extracted from URL: %s", header["username"])

        except YouTubeScraperError:
            raise
        except Exception as exc:
            elapsed = time.monotonic() - t0
            logger.exception("Unexpected error after %.2fs scraping %s", elapsed, channel_url)
            raise YouTubeScraperError(
                f"Scraping failed for {channel_url}: {exc}"
            ) from exc
        finally:
            await browser.close()
            logger.info("Browser closed")

    # ── Assemble result ─────────────────────────────────────────────────────
    optional_fields = [
        "views", "joined_date", "website",
        "facebook", "instagram", "twitter", "tiktok", "community",
    ]
    channel: dict[str, Any] = {
        "picture_url": header.get("picture_url"),
        "name": header.get("name"),
        "username": header.get("username"),
        "subscribers": header.get("subscribers"),
        "videos": header.get("videos"),
        "views": about_data.get("views"),
        "joined_date": about_data.get("joined_date"),
        "description": about_data.get("description"),
        "website": about_data.get("website"),
        "facebook": about_data.get("facebook"),
        "instagram": about_data.get("instagram"),
        "twitter": about_data.get("twitter"),
        "tiktok": about_data.get("tiktok"),
        "community": about_data.get("community"),
    }

    for field in optional_fields:
        if channel[field] is None:
            logger.info("Optional field '%s' unavailable for %s", field, channel_url)

    elapsed = time.monotonic() - t0
    logger.info(
        "Scraping finished in %.2fs | name=%s | subscribers=%s | username=%s",
        elapsed,
        channel.get("name"),
        channel.get("subscribers"),
        channel.get("username"),
    )

    return {
        "platform": "youtube",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "channel": channel,
    }


# ---------------------------------------------------------------------------
# Public synchronous entry point (called from Airflow task)
# ---------------------------------------------------------------------------

def run_scraper(channel_url: str) -> dict:
    """
    Scrape a YouTube channel and return its metadata.

    Args:
        channel_url: Full YouTube channel URL.
                     Accepts /@handle, /c/name, /channel/ID, /user/name formats.

    Returns:
        Dict with keys: platform, scraped_at, channel (nested dict).

    Raises:
        YouTubeScraperError: If scraping fails.
        ValueError:          If channel_url is empty.
    """
    if not channel_url:
        raise ValueError("channel_url must not be empty")

    if not channel_url.startswith("http"):
        channel_url = f"https://www.youtube.com/{channel_url.lstrip('/')}"

    logger.info("run_scraper invoked | url=%s", channel_url)
    return asyncio.run(_scrape_async(channel_url))

if __name__ == "__main__":
    import json
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    url = "https://www.youtube.com/@gamingdoseth"   # change to your channel

    try:
        result = run_scraper(url)
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(e)