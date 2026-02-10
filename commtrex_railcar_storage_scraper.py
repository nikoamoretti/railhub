#!/usr/bin/env python3
"""
Commtrex Railcar Storage Facility Scraper using FlareSolverr

Prerequisites:
  docker run -d --name flaresolverr -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest

Usage:
  python3 commtrex_railcar_storage_scraper.py                  # scrape all
  python3 commtrex_railcar_storage_scraper.py --limit 10       # test with 10
  python3 commtrex_railcar_storage_scraper.py --resume          # resume interrupted scrape
"""

import csv
import json
import logging
import re
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
FLARESOLVERR_URL = "http://localhost:8191/v1"
URLS_FILE = "commtrex_railcar_storage_urls.txt"
OUTPUT_CSV = "commtrex_railcar_storage.csv"
PROGRESS_FILE = "commtrex_railcar_storage_progress.json"
DELAY_BETWEEN_REQUESTS = 2
MAX_RETRIES = 3
REQUEST_TIMEOUT = 45
FLARESOLVERR_MAX_TIMEOUT = 30000

CSV_FIELDS = [
    "id", "name", "location", "city", "state",
    "interchange_railroads", "capacity", "hazmat_suited",
    "rail_services", "facility_types", "track_types",
    "about", "url", "scraped_at",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("commtrex_railcar_storage.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FlareSolverr client (same as transload scraper)
# ---------------------------------------------------------------------------

def flaresolverr_get(url: str, session_id: str = None) -> dict | None:
    payload = {
        "cmd": "request.get",
        "url": url,
        "maxTimeout": FLARESOLVERR_MAX_TIMEOUT,
    }
    if session_id:
        payload["session"] = session_id

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(FLARESOLVERR_URL, json=payload, timeout=REQUEST_TIMEOUT)
            data = r.json()
            if data.get("status") == "ok":
                return data["solution"]
            log.warning("FlareSolverr error on %s (attempt %d): %s", url, attempt, data.get("message"))
        except requests.exceptions.Timeout:
            log.warning("Timeout fetching %s (attempt %d/%d)", url, attempt, MAX_RETRIES)
        except Exception as e:
            log.warning("Error fetching %s (attempt %d/%d): %s", url, attempt, MAX_RETRIES, e)

        if attempt < MAX_RETRIES:
            time.sleep(DELAY_BETWEEN_REQUESTS * attempt)

    return None


def create_session() -> str:
    try:
        r = requests.post(FLARESOLVERR_URL, json={"cmd": "sessions.create"}, timeout=15)
        session_id = r.json().get("session", "")
        if session_id:
            log.info("Created FlareSolverr session: %s", session_id)
        return session_id
    except Exception as e:
        log.warning("Could not create session: %s", e)
        return ""


def destroy_session(session_id: str):
    try:
        requests.post(FLARESOLVERR_URL, json={"cmd": "sessions.destroy", "session": session_id}, timeout=10)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# HTML parser for railcar storage pages
# ---------------------------------------------------------------------------

def parse_railcar_storage(html: str, url: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else ""
    if "50x" in title or "404" in title:
        return None

    # --- Extract ID from URL ---
    m = re.search(r"/location/(\d+)\.html", url)
    fac_id = m.group(1) if m else ""

    # --- Name from .storage-location-name ---
    name_el = soup.find("div", class_="storage-location-name")
    name = name_el.get_text(strip=True) if name_el else ""
    # Clean up trailing "Railcar Storage Facility"
    name = re.sub(r"\s*Railcar Storage Facility\s*$", "", name).strip()
    if not name and title:
        name = title.split("|")[0].strip().replace(" Railcar Storage Facility", "").strip()

    # --- Storage Facility Information (label/value pairs) ---
    info = {}
    for c in soup.find_all("div", class_="storage-location-details-container"):
        if "Storage Facility Information" in c.get_text()[:200]:
            for label_el in c.find_all("label", class_="field-label"):
                label_text = label_el.get_text(strip=True)
                row = label_el.find_parent("div", class_="row")
                if row:
                    display = row.find("div", class_="display")
                    if display:
                        info[label_text] = display.get_text(strip=True)
            break

    location = info.get("Location", "")
    interchange = info.get("Interchange", "")
    capacity = info.get("Capacity", "")
    hazmat = info.get("Suited for HazMat", "")

    # Parse city/state from location
    city, state = "", ""
    if "," in location:
        parts = [p.strip() for p in location.split(",")]
        city = parts[0]
        state = parts[1] if len(parts) > 1 else ""

    # --- Facility Characteristics (details-box sections) ---
    def get_box_values(heading_text: str) -> str:
        skip = {"See more", "See less", "see more", "see less"}
        for box in soup.find_all("div", class_="details-box"):
            title_div = box.find("div", class_="title")
            if title_div and heading_text.lower() in title_div.get_text(strip=True).lower():
                values = []
                for child in title_div.find_next_siblings("div"):
                    t = child.get_text(strip=True)
                    if t and t not in skip:
                        t = re.sub(r"See (?:more|less)$", "", t).strip()
                        if t:
                            values.append(t)
                return "; ".join(values)
        return ""

    rail_services = get_box_values("Rail Services Offered")
    facility_types = get_box_values("Facility Type")
    track_types = get_box_values("Track Type")

    # --- About ---
    about = ""
    for c in soup.find_all("div", class_="storage-location-details-container"):
        text = c.get_text()
        if "About" in text[:30]:
            p = c.find("p")
            if p:
                about = p.get_text(strip=True)
            break

    return {
        "id": fac_id,
        "name": name,
        "location": location,
        "city": city,
        "state": state,
        "interchange_railroads": interchange,
        "capacity": capacity,
        "hazmat_suited": hazmat,
        "rail_services": rail_services,
        "facility_types": facility_types,
        "track_types": track_types,
        "about": about,
        "url": url,
        "scraped_at": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

def load_progress() -> set:
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE) as f:
            data = json.load(f)
            return set(data.get("completed", []))
    return set()


def save_progress(completed: set, failed: list):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({
            "completed": list(completed),
            "failed": failed,
            "last_updated": datetime.utcnow().isoformat(),
            "total_completed": len(completed),
            "total_failed": len(failed),
        }, f)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scrape Commtrex railcar storage facilities via FlareSolverr")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of facilities (0=all)")
    parser.add_argument("--resume", action="store_true", help="Resume from previous progress")
    parser.add_argument("--delay", type=float, default=DELAY_BETWEEN_REQUESTS, help="Delay between requests")
    parser.add_argument("--urls-file", default=URLS_FILE, help="File with facility URLs")
    parser.add_argument("--output", default=OUTPUT_CSV, help="Output CSV file")
    args = parser.parse_args()

    urls_path = Path(args.urls_file)
    if not urls_path.exists():
        log.error("URLs file not found: %s", args.urls_file)
        sys.exit(1)

    urls = [line.strip() for line in urls_path.read_text().splitlines() if line.strip()]
    log.info("Loaded %d railcar storage URLs from %s", len(urls), args.urls_file)

    completed = load_progress() if args.resume else set()
    failed = []

    if completed:
        log.info("Resuming: %d already completed, %d remaining", len(completed), len(urls) - len(completed))

    pending = [u for u in urls if u not in completed]
    if args.limit:
        pending = pending[:args.limit]

    log.info("Will scrape %d railcar storage facilities", len(pending))

    output_path = Path(args.output)
    write_header = not output_path.exists() or not args.resume

    csv_file = open(output_path, "a" if args.resume else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
    if write_header:
        writer.writeheader()

    session_id = create_session()

    success_count = 0
    fail_count = 0
    start_time = time.time()

    try:
        for i, url in enumerate(pending, 1):
            elapsed = time.time() - start_time
            rate = success_count / (elapsed / 60) if elapsed > 60 else 0
            log.info("[%d/%d] (ok=%d fail=%d rate=%.0f/min) %s",
                     i, len(pending), success_count, fail_count, rate, url)

            sol = flaresolverr_get(url, session_id)
            if not sol:
                fail_count += 1
                failed.append({"url": url, "error": "flaresolverr_failed"})
                log.warning("  FAILED: no response")
                time.sleep(args.delay)
                continue

            html = sol.get("response", "")
            final_url = sol.get("url", url)

            if "error/50x" in final_url or "error/404" in final_url:
                fail_count += 1
                failed.append({"url": url, "error": "server_error", "redirect": final_url})
                log.warning("  FAILED: server error -> %s", final_url)
                completed.add(url)
                time.sleep(args.delay)
                continue

            facility = parse_railcar_storage(html, url)
            if facility:
                writer.writerow(facility)
                csv_file.flush()
                success_count += 1
                completed.add(url)
                log.info("  OK: %s - %s, %s (%s)", facility["name"], facility["city"], facility["state"], facility["capacity"])
            else:
                fail_count += 1
                failed.append({"url": url, "error": "parse_failed"})
                log.warning("  FAILED: could not parse")

            if i % 25 == 0:
                save_progress(completed, failed)
                log.info("  Progress saved (%d completed)", len(completed))

            time.sleep(args.delay)

    except KeyboardInterrupt:
        log.info("\nInterrupted. Saving progress...")
    finally:
        csv_file.close()
        save_progress(completed, failed)
        destroy_session(session_id)

    elapsed = time.time() - start_time
    log.info("=" * 60)
    log.info("RAILCAR STORAGE SCRAPE COMPLETE")
    log.info("  Total:     %d", len(pending))
    log.info("  Success:   %d", success_count)
    log.info("  Failed:    %d", fail_count)
    log.info("  Duration:  %.1f minutes", elapsed / 60)
    log.info("  Output:    %s", args.output)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
