#!/usr/bin/env python3
"""
Commtrex Facility Count Verification using ScrapingBee
Scrapes the actual Commtrex website to verify facility counts per state
"""

import os
import re
import json
import time
import requests
from bs4 import BeautifulSoup
from collections import defaultdict
from typing import Dict, List, Optional
from urllib.parse import urljoin

# ScrapingBee API configuration
SCRAPINGBEE_API_KEY = os.getenv('SCRAPINGBEE_API_KEY')
SCRAPINGBEE_API_URL = "https://app.scrapingbee.com/api/v1/"

# Commtrex URLs
COMMTREX_MAIN_URL = "https://www.commtrex.com/transloading.html"
COMMTREX_STATE_BASE = "https://www.commtrex.com/transloading/{}.html"


def scrape_with_scrapingbee(url: str, wait: int = 2000, retries: int = 3) -> Optional[str]:
    """
    Scrape URL using ScrapingBee API
    
    Args:
        url: Target URL to scrape
        wait: Time to wait for JS rendering (ms)
        retries: Number of retry attempts
    
    Returns:
        HTML content or None if failed
    """
    if not SCRAPINGBEE_API_KEY:
        raise ValueError("SCRAPINGBEE_API_KEY environment variable not set")
    
    params = {
        'api_key': SCRAPINGBEE_API_KEY,
        'url': url,
        'wait': wait,
        'block_ads': 'true',
        'premium_proxy': 'true',  # Use premium proxies for Cloudflare bypass
    }
    
    for attempt in range(retries):
        try:
            print(f"  Scraping {url} (attempt {attempt + 1}/{retries})...")
            response = requests.get(SCRAPINGBEE_API_URL, params=params, timeout=60)
            
            if response.status_code == 200:
                # Verify we got real content
                if len(response.text) > 1000 and 'commtrex' in response.text.lower():
                    return response.text
                else:
                    print(f"    ⚠ Unexpected content length: {len(response.text)}")
            else:
                print(f"    ✗ HTTP {response.status_code}")
                
        except Exception as e:
            print(f"    ✗ Error: {e}")
        
        if attempt < retries - 1:
            time.sleep(2 ** attempt)  # Exponential backoff
    
    return None


def parse_main_page(html: str) -> Dict[str, Dict]:
    """
    Parse the main transloading page to get state counts
    
    Returns:
        Dict mapping state codes to {name, url, count, cities: []}
    """
    soup = BeautifulSoup(html, 'html.parser')
    states_data = {}
    
    # Look for state links and counts
    # Common patterns: links like /transloading/ca.html, /transloading/tx.html
    state_pattern = re.compile(r'/transloading/([a-z]{2})\.html')
    
    # Find all links to state pages
    for link in soup.find_all('a', href=True):
        href = link['href']
        match = state_pattern.search(href)
        
        if match:
            state_code = match.group(1)
            state_name = link.get_text(strip=True)
            
            # Clean up state name
            state_name = re.sub(r'\s+\(\d+\)$', '', state_name)  # Remove "(count)" suffix
            state_name = state_name.replace('Transloading in ', '').strip()
            
            if state_code not in states_data:
                states_data[state_code] = {
                    'name': state_name,
                    'url': urljoin(COMMTREX_MAIN_URL, href),
                    'count': 0,
                    'cities': []
                }
    
    return states_data


def parse_state_page(html: str, state_code: str) -> Dict:
    """
    Parse a state page to get facility/city counts
    
    Returns:
        Dict with {cities: [], facilities_count: int}
    """
    soup = BeautifulSoup(html, 'html.parser')
    
    result = {
        'cities': [],
        'facilities_count': 0
    }
    
    # Look for city links - pattern: /transloading/{state}-{city}.html
    city_pattern = re.compile(rf'/transloading/{state_code}-([^/]+)\.html')
    
    for link in soup.find_all('a', href=True):
        href = link['href']
        match = city_pattern.search(href)
        
        if match:
            city_name = match.group(1).replace('-', ' ').title()
            if city_name not in [c['name'] for c in result['cities']]:
                result['cities'].append({
                    'name': city_name,
                    'url': urljoin(COMMTREX_MAIN_URL, href)
                })
    
    # Also look for facility listings
    # Facilities might be listed directly on state page
    facility_selectors = [
        '.facility', '.facility-item', '.listing', '.company',
        '[class*="facility"]', '[class*="listing"]'
    ]
    
    facilities_found = 0
    for selector in facility_selectors:
        elements = soup.select(selector)
        if len(elements) > facilities_found:
            facilities_found = len(elements)
    
    result['facilities_count'] = max(facilities_found, len(result['cities']))
    
    return result


def count_facilities_in_city(html: str) -> int:
    """Count facilities on a city page"""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Look for facility listings
    count = 0
    
    # Common patterns for facility listings
    selectors = [
        '.facility', '.facility-item', '.company-card',
        '.listing-item', '[class*="facility"]', 
        '.transloading-facility', '.location-card'
    ]
    
    for selector in selectors:
        elements = soup.select(selector)
        if len(elements) > count:
            count = len(elements)
    
    # Also count by headings that might be facility names
    if count == 0:
        headings = soup.find_all(['h2', 'h3', 'h4'])
        count = len([h for h in headings if h.get_text(strip=True)])
    
    return count


def verify_state_counts(states_to_check: List[str] = None) -> Dict:
    """
    Verify facility counts for all states or specific states
    
    Args:
        states_to_check: List of state codes to check, or None for all found
    
    Returns:
        Dict with verification results
    """
    print("="*70)
    print("COMMTREX FACILITY COUNT VERIFICATION")
    print("Using ScrapingBee API")
    print("="*70)
    
    # Step 1: Scrape main page to get state list
    print("\n[1/3] Scraping main page to get state list...")
    main_html = scrape_with_scrapingbee(COMMTREX_MAIN_URL, wait=3000)
    
    if not main_html:
        print("✗ Failed to scrape main page")
        return {}
    
    print("✓ Main page scraped successfully")
    
    states_data = parse_main_page(main_html)
    print(f"  Found {len(states_data)} states on main page")
    
    # Filter if specific states requested
    if states_to_check:
        states_data = {k: v for k, v in states_data.items() if k in states_to_check}
    
    # Step 2: Scrape each state page to get city/facility counts
    print(f"\n[2/3] Scraping {len(states_data)} state pages for detailed counts...")
    
    results = {}
    total_facilities = 0
    
    for i, (state_code, state_info) in enumerate(sorted(states_data.items()), 1):
        print(f"\n  [{i}/{len(states_data)}] {state_info['name']} ({state_code.upper()})")
        
        state_url = COMMTREX_STATE_BASE.format(state_code)
        state_html = scrape_with_scrapingbee(state_url, wait=3000)
        
        if state_html:
            state_data = parse_state_page(state_html, state_code)
            
            results[state_code] = {
                'name': state_info['name'],
                'cities_count': len(state_data['cities']),
                'facilities_estimate': state_data['facilities_count'],
                'cities': state_data['cities']
            }
            
            # If we found cities, sample one to count facilities
            if state_data['cities']:
                sample_city = state_data['cities'][0]
                print(f"    Sampling city: {sample_city['name']}...")
                
                city_html = scrape_with_scrapingbee(sample_city['url'], wait=3000)
                if city_html:
                    city_facilities = count_facilities_in_city(city_html)
                    results[state_code]['facilities_per_city_sample'] = city_facilities
                    # Estimate total based on sample
                    estimated_total = city_facilities * len(state_data['cities'])
                    results[state_code]['estimated_total_facilities'] = estimated_total
                    total_facilities += estimated_total
                    print(f"    ✓ {len(state_data['cities'])} cities, ~{estimated_total} facilities")
                else:
                    results[state_code]['estimated_total_facilities'] = len(state_data['cities'])
                    total_facilities += len(state_data['cities'])
                    print(f"    ✓ {len(state_data['cities'])} cities")
            else:
                print(f"    ⚠ No cities found")
        else:
            print(f"    ✗ Failed to scrape state page")
            results[state_code] = {
                'name': state_info['name'],
                'error': 'Failed to scrape'
            }
        
        # Be nice to the API
        time.sleep(1)
    
    # Step 3: Compile results
    print(f"\n[3/3] Compiling results...")
    
    return {
        'total_states': len(results),
        'total_facilities_estimate': total_facilities,
        'states': results,
        'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S')
    }


def print_results(results: Dict):
    """Print results in a formatted table"""
    print("\n" + "="*70)
    print("VERIFICATION RESULTS")
    print("="*70)
    
    if not results or 'states' not in results:
        print("No results available")
        return
    
    print(f"\n{'State':<20} {'Code':<6} {'Cities':<8} {'Est. Facilities':<15}")
    print("-"*70)
    
    total_cities = 0
    total_facilities = 0
    
    for state_code in sorted(results['states'].keys()):
        data = results['states'][state_code]
        name = data.get('name', 'Unknown')
        cities = data.get('cities_count', 0)
        facilities = data.get('estimated_total_facilities', 0)
        
        print(f"{name:<20} {state_code.upper():<6} {cities:<8} {facilities:<15,}")
        
        total_cities += cities
        total_facilities += facilities
    
    print("-"*70)
    print(f"{'TOTAL':<20} {'':<6} {total_cities:<8} {total_facilities:<15,}")
    print("="*70)
    
    print(f"\nComparison to previous count (549):")
    print(f"  Previous count: 549")
    print(f"  New estimate:   {total_facilities:,}")
    print(f"  Difference:     {total_facilities - 549:,}")
    
    if total_facilities > 549 * 10:
        print(f"\n⚠ WARNING: New count is MUCH higher than previous 549!")
        print(f"   The previous scrape was likely incomplete.")
    elif total_facilities < 549:
        print(f"\n⚠ WARNING: New count is LOWER than previous 549!")
        print(f"   This estimate may be missing some facilities.")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Verify Commtrex facility counts using ScrapingBee'
    )
    parser.add_argument(
        '--states', '-s', 
        nargs='+',
        help='Specific state codes to check (e.g., tx ca ny)'
    )
    parser.add_argument(
        '--output', '-o',
        default='commtrex_verification.json',
        help='Output JSON file for results'
    )
    parser.add_argument(
        '--api-key',
        help='ScrapingBee API key (or set SCRAPINGBEE_API_KEY env var)'
    )
    
    args = parser.parse_args()
    
    # Set API key if provided
    if args.api_key:
        global SCRAPINGBEE_API_KEY
        SCRAPINGBEE_API_KEY = args.api_key
    
    # Check for API key
    if not SCRAPINGBEE_API_KEY:
        print("✗ ScrapingBee API key required")
        print("\nOptions:")
        print("1. Set SCRAPINGBEE_API_KEY environment variable")
        print("2. Pass --api-key flag")
        print("\nGet your API key at: https://www.scrapingbee.com/")
        return 1
    
    # Run verification
    results = verify_state_counts(args.states)
    
    # Print results
    print_results(results)
    
    # Save to file
    if results:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n✓ Results saved to: {args.output}")
    
    return 0


if __name__ == '__main__':
    exit(main())
