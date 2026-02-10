#!/usr/bin/env python3
"""
Debug script to understand Commtrex page structure
"""

import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

COMMTREX_MAIN_URL = "https://www.commtrex.com/transloading.html"

async def debug_page():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        )
        page = await context.new_page()
        
        print("Loading main page...")
        await page.goto(COMMTREX_MAIN_URL, wait_until='networkidle', timeout=60000)
        await asyncio.sleep(3)  # Wait for any JS to load
        
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        print("\n=== PAGE TITLE ===")
        title = soup.find('title')
        print(title.get_text() if title else "No title")
        
        print("\n=== ALL LINKS (first 30) ===")
        links = soup.find_all('a', href=True)
        for i, link in enumerate(links[:30]):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            if '/transloading/' in href:
                print(f"{i+1}. {href} - '{text}'")
        
        print("\n=== STATE-LIKE LINKS ===")
        import re
        state_pattern = re.compile(r'/transloading/([a-z]{2})\.html')
        for link in links:
            href = link.get('href', '')
            if state_pattern.search(href):
                print(f"  {href} - '{link.get_text(strip=True)}'")
        
        print("\n=== LOOKING FOR STATE LIST CONTAINER ===")
        # Look for common container classes
        containers = soup.find_all(['div', 'ul', 'section'], class_=re.compile(r'state|location|region', re.I))
        print(f"Found {len(containers)} potential containers")
        for i, container in enumerate(containers[:5]):
            print(f"\nContainer {i+1}: {container.name} class={container.get('class')}")
            print(f"  Text preview: {container.get_text(strip=True)[:200]}...")
        
        print("\n=== TEXAS PAGE SAMPLE ===")
        tx_url = "https://www.commtrex.com/transloading/tx.html"
        await page.goto(tx_url, wait_until='networkidle', timeout=60000)
        await asyncio.sleep(2)
        
        tx_content = await page.content()
        tx_soup = BeautifulSoup(tx_content, 'html.parser')
        
        print(f"Title: {tx_soup.find('title').get_text() if tx_soup.find('title') else 'No title'}")
        
        # Look for city links
        city_pattern = re.compile(r'/transloading/tx-([a-z-]+)\.html')
        city_links = []
        for link in tx_soup.find_all('a', href=True):
            href = link.get('href', '')
            match = city_pattern.search(href)
            if match:
                city_links.append((match.group(1), link.get_text(strip=True)))
        
        print(f"\nFound {len(city_links)} city links:")
        for city, text in city_links[:20]:
            print(f"  {city} - '{text}'")
        
        print("\n=== LOOKING FOR FACILITY ELEMENTS ===")
        facility_selectors = [
            '.facility', '.facility-item', '.company', '.listing',
            '[class*="facility"]', '[class*="company"]', '.card'
        ]
        for selector in facility_selectors:
            elements = tx_soup.select(selector)
            if elements:
                print(f"  {selector}: {len(elements)} elements")
        
        # Look at headings
        headings = tx_soup.find_all(['h1', 'h2', 'h3'])
        print(f"\nHeadings found: {len(headings)}")
        for h in headings[:10]:
            print(f"  {h.name}: {h.get_text(strip=True)[:80]}")
        
        await browser.close()

if __name__ == '__main__':
    asyncio.run(debug_page())
