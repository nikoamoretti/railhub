#!/usr/bin/env python3
"""
Commtrex Facility Scraper using FlareSolverr
Bypasses Cloudflare protection via FlareSolverr Docker container.

Prerequisites:
  docker run -d --name flaresolverr -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest

Usage:
  python3 commtrex_flaresolverr_scraper.py                    # scrape all
  python3 commtrex_flaresolverr_scraper.py --limit 10         # test with 10
  python3 commtrex_flaresolverr_scraper.py --resume            # resume interrupted scrape
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
URLS_FILE = "commtrex_facility_urls.txt"
OUTPUT_CSV = "commtrex_facilities.csv"
PROGRESS_FILE = "commtrex_scrape_progress.json"
DELAY_BETWEEN_REQUESTS = 2  # seconds - be respectful
MAX_RETRIES = 3
REQUEST_TIMEOUT = 45  # seconds per request to FlareSolverr
FLARESOLVERR_MAX_TIMEOUT = 30000  # ms - FlareSolverr internal timeout

CSV_FIELDS = [
    "id", "name", "street_address", "city", "state", "zip_code", "country",
    "phone", "description", "product_types", "hazmat_handling",
    "transfer_modes", "railroads", "track_capacity", "security_features",
    "equipment", "cities_served", "storage_options", "heating_capabilities",
    "kosher_certification", "onsite_railcar_storage", "onsite_scale",
    "weight_restricted_263k", "weight_restricted_286k",
    "hours_mon", "hours_tue", "hours_wed", "hours_thu",
    "hours_fri", "hours_sat", "hours_sun",
    "about", "url", "scraped_at",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("commtrex_flaresolverr.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FlareSolverr client
# ---------------------------------------------------------------------------

def flaresolverr_get(url: str, session_id: str = None) -> dict | None:
    """Fetch a URL via FlareSolverr. Returns solution dict or None."""
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
    """Create a FlareSolverr session for cookie persistence."""
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
# HTML parser
# ---------------------------------------------------------------------------

def parse_facility(html: str, url: str) -> dict | None:
    """Parse a Commtrex facility page into a flat dict."""
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else ""
    if "50x" in title or "error" in title.lower() or "404" in title:
        return None

    # --- JSON-LD for core identity ---
    name = ""
    phone = ""
    street = ""
    locality = ""
    region = ""
    postal = ""
    country = "US"
    ld_desc = ""

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            if ld.get("@type") == "LocalBusiness":
                name = ld.get("name", "")
                phone = ld.get("telephone", "")
                ld_desc = ld.get("description", "")
                addr = ld.get("address", {})
                street = addr.get("streetAddress", "")
                locality = addr.get("addressLocality", "")
                region = addr.get("addressRegion", "")
                postal = addr.get("postalCode", "")
                country = addr.get("addressCountry", "US")
        except (json.JSONDecodeError, TypeError):
            continue

    # Fallback name from title
    if not name and title:
        name = title.split("|")[0].strip()

    # --- Extract ID from URL ---
    m = re.search(r"/location/(\d+)\.html", url)
    fac_id = m.group(1) if m else ""

    # --- details-box sections ---
    def get_box_values(heading_text: str) -> list[str]:
        """Find a details-box by its title text and return child div values."""
        skip = {"See more", "See less", "see more", "see less"}
        for box in soup.find_all("div", class_="details-box"):
            title_div = box.find("div", class_="title")
            if title_div and heading_text.lower() in title_div.get_text(strip=True).lower():
                values = []
                for child in title_div.find_next_siblings("div"):
                    t = child.get_text(strip=True)
                    if t and t not in skip:
                        # Clean any trailing "See less"/"See more" from text
                        t = re.sub(r"See (?:more|less)$", "", t).strip()
                        if t:
                            values.append(t)
                return values
        return []

    product_types = get_box_values("Product Types Handled")
    hazmat_vals = get_box_values("Hazardous Material Handling")
    hazmat = hazmat_vals[0] if hazmat_vals else ""
    transfer_modes = get_box_values("Transfer Modes")
    railroads = get_box_values("Serving Class I Railroads")
    track_vals = get_box_values("Track Capacity")
    track_capacity = track_vals[0] if track_vals else ""
    security = get_box_values("Security")
    equipment = get_box_values("Transload Equipment")
    cities_served = get_box_values("Cities Served")

    # --- Additional Services (label/value row pairs) ---
    additional = {}
    for container in soup.find_all("div", class_="transload-location-details-container"):
        if "Additional Services" not in container.get_text()[:200]:
            continue
        for label_el in container.find_all("label", class_="field-label"):
            label_text = label_el.get_text(strip=True)
            # Value is in a sibling div.display within the same row
            row = label_el.find_parent("div", class_="row")
            if row:
                display = row.find("div", class_="display")
                if display:
                    additional[label_text] = display.get_text(strip=True)
        break

    storage_options = additional.get("Product Storage Options", "")
    heating = additional.get("Heating Capabilities", "")
    kosher = additional.get("Kosher Certification", "")
    railcar_storage = additional.get("Onsite Railcar Storage", "")
    onsite_scale = additional.get("Onsite Scale", "")
    weight_263 = additional.get("Weight Restricted 263k", "")
    weight_286 = additional.get("Weight Restricted 286k", "")

    # --- Hours of Operation ---
    hours = {"mon": "", "tue": "", "wed": "", "thu": "", "fri": "", "sat": "", "sun": ""}
    day_map = {
        "monday": "mon", "tuesday": "tue", "wednesday": "wed",
        "thursday": "thu", "friday": "fri", "saturday": "sat", "sunday": "sun",
    }
    for container in soup.find_all("div", class_="transload-location-details-container"):
        container_text = container.get_text()
        if "Hours of Operation" in container_text or "Operating Hours" in container_text:
            for label_el in container.find_all("label", class_="field-label"):
                day_name = label_el.get_text(strip=True).lower()
                day_key = day_map.get(day_name, "")
                if day_key:
                    row = label_el.find_parent("div", class_="row")
                    if row:
                        display = row.find("div", class_="display")
                        if display:
                            hours[day_key] = display.get_text(strip=True)
            break

    # --- About ---
    about = ""
    for container in soup.find_all("div", class_="transload-location-details-container"):
        h3 = container.find("h3")
        if h3 and "about" in h3.get_text(strip=True).lower():
            p = container.find("p")
            if p:
                about = p.get_text(strip=True)
            break

    return {
        "id": fac_id,
        "name": name,
        "street_address": street,
        "city": locality,
        "state": region,
        "zip_code": postal,
        "country": country,
        "phone": phone,
        "description": ld_desc,
        "product_types": "; ".join(product_types),
        "hazmat_handling": hazmat,
        "transfer_modes": "; ".join(transfer_modes),
        "railroads": "; ".join(railroads),
        "track_capacity": track_capacity,
        "security_features": "; ".join(security),
        "equipment": "; ".join(equipment),
        "cities_served": "; ".join(cities_served),
        "storage_options": storage_options,
        "heating_capabilities": heating,
        "kosher_certification": kosher,
        "onsite_railcar_storage": railcar_storage,
        "onsite_scale": onsite_scale,
        "weight_restricted_263k": weight_263,
        "weight_restricted_286k": weight_286,
        "hours_mon": hours["mon"],
        "hours_tue": hours["tue"],
        "hours_wed": hours["wed"],
        "hours_thu": hours["thu"],
        "hours_fri": hours["fri"],
        "hours_sat": hours["sat"],
        "hours_sun": hours["sun"],
        "about": about,
        "url": url,
        "scraped_at": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

def load_progress() -> set:
    """Load set of already-scraped URLs."""
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
    parser = argparse.ArgumentParser(description="Scrape Commtrex facilities via FlareSolverr")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of facilities to scrape (0=all)")
    parser.add_argument("--resume", action="store_true", help="Resume from previous progress")
    parser.add_argument("--delay", type=float, default=DELAY_BETWEEN_REQUESTS, help="Delay between requests in seconds")
    parser.add_argument("--urls-file", default=URLS_FILE, help="File with facility URLs")
    parser.add_argument("--output", default=OUTPUT_CSV, help="Output CSV file")
    args = parser.parse_args()

    # Load URLs
    urls_path = Path(args.urls_file)
    if not urls_path.exists():
        log.error("URLs file not found: %s", args.urls_file)
        sys.exit(1)

    urls = [line.strip() for line in urls_path.read_text().splitlines() if line.strip()]
    log.info("Loaded %d facility URLs from %s", len(urls), args.urls_file)

    # Load progress
    completed = load_progress() if args.resume else set()
    failed = []

    if completed:
        log.info("Resuming: %d already completed, %d remaining", len(completed), len(urls) - len(completed))

    # Filter out completed
    pending = [u for u in urls if u not in completed]
    if args.limit:
        pending = pending[:args.limit]

    log.info("Will scrape %d facilities", len(pending))

    # Prepare CSV
    output_path = Path(args.output)
    write_header = not output_path.exists() or not args.resume

    csv_file = open(output_path, "a" if args.resume else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
    if write_header:
        writer.writeheader()

    # Create FlareSolverr session
    session_id = create_session()

    # Scrape
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
                log.warning("  FAILED: no response from FlareSolverr")
                time.sleep(args.delay)
                continue

            html = sol.get("response", "")
            final_url = sol.get("url", url)

            # Check for error/redirect
            if "error/50x" in final_url or "error/404" in final_url:
                fail_count += 1
                failed.append({"url": url, "error": "server_error", "redirect": final_url})
                log.warning("  FAILED: server error (redirected to %s)", final_url)
                completed.add(url)  # Don't retry server errors
                time.sleep(args.delay)
                continue

            facility = parse_facility(html, url)
            if facility:
                writer.writerow(facility)
                csv_file.flush()
                success_count += 1
                completed.add(url)
                log.info("  OK: %s - %s, %s", facility["name"], facility["city"], facility["state"])
            else:
                fail_count += 1
                failed.append({"url": url, "error": "parse_failed"})
                log.warning("  FAILED: could not parse facility data")

            # Save progress periodically
            if i % 25 == 0:
                save_progress(completed, failed)
                log.info("  Progress saved (%d completed)", len(completed))

            time.sleep(args.delay)

    except KeyboardInterrupt:
        log.info("\nInterrupted by user. Saving progress...")
    finally:
        csv_file.close()
        save_progress(completed, failed)
        destroy_session(session_id)

    elapsed = time.time() - start_time
    log.info("=" * 60)
    log.info("SCRAPE COMPLETE")
    log.info("  Total:     %d", len(pending))
    log.info("  Success:   %d", success_count)
    log.info("  Failed:    %d", fail_count)
    log.info("  Duration:  %.1f minutes", elapsed / 60)
    log.info("  Output:    %s", args.output)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
