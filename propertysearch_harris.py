import asyncio
import math
import asyncio
import csv
from datetime import datetime
import os
from crawl4ai import AsyncWebCrawler, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
from playwright.async_api import Page, BrowserContext

# Target URL with page size parameter
BASE_URL = "https://harris.trueprodigy-taxtransparency.com/taxTransparency/propertySearch?pageSize=100"

# Constants
CONCURRENT_TASKS_LIMIT = 50  # Number of concurrent tasks

# Configuration for search range
START_RANGE = 102976
END_RANGE = 103177
BATCH_SIZE = 100  # How many numbers to process before saving progress

async def scrape_properties(search_term, on_page, total_pages):
    import json

    browser_config = BrowserConfig(
        headless=True
    )

    crawler_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_for='css:table tr',
        extraction_strategy=JsonCssExtractionStrategy(
            schema={
                "name": "Property Search Results",
                "baseSelector": "table tr",
                "fields": [
                    {
                        "name": "account",
                        "selector": "td:nth-child(1)",
                        "type": "text"
                    },
                    {
                        "name": "owner_name",
                        "selector": "td:nth-child(2)",
                        "type": "text"
                    },
                    {
                        "name": "situs",
                        "selector": "td:nth-child(3)",
                        "type": "text"
                    },
                    {
                        "name": "dba",
                        "selector": "td:nth-child(4)",
                        "type": "text"
                    }
                ]
            }
        )
    )

    detected_pages = None

    async def after_goto(page: Page, context: BrowserContext, url: str, response: dict, **kwargs):
        """Hook called after navigating to each URL"""
        print(f"[HOOK] after_goto - Successfully loaded: {url}")
        nonlocal detected_pages

        try:
            # Wait for search box to be available
            input_field = await page.wait_for_selector('input[type="text"][placeholder="Search by Name, Address or Property ID (PID#), for example \\"William Travis\\" or \\"1100 Congress Ave\\" or \\"151999\\""]', timeout=10000)
            
            # Type the search query
            await input_field.fill(search_term)
            
            # Click search button
            search_button = await page.wait_for_selector('button[type="submit"], button[aria-label="Search"], button.jss70.jss44.jss55.jss56.jss58.jss59', timeout=10000)
            await search_button.click()
            
            # Wait for results
            await page.wait_for_selector('table tr', timeout=10000)
            print("[HOOK] Search completed and results loaded!")

            await page.wait_for_timeout(2000)

            if on_page == 1:
                try:
                    page_links = await page.query_selector_all('//li[@class="next"]/preceding-sibling::li/a')
                    if page_links:
                        page_numbers = []
                        for link in page_links:
                            text = await page.evaluate('(el) => el.textContent', link)
                            if text.isdigit():
                                page_numbers.append(int(text))
                        if page_numbers:
                            detected_pages = max(page_numbers)
                            print(f"Detected {detected_pages} total pages")
                except Exception as e:
                    print(f"Error detecting pagination: {str(e)}")
                    detected_pages = 1
            
            if on_page > 1:
                try:
                    page_link_selector = f'//a[@role="button" and @class="btn" and @aria-label="Page {on_page}"]'
                    page_link = await page.wait_for_selector(page_link_selector, timeout=5000)
                    
                    if page_link:
                        await page_link.click()
                        await page.wait_for_selector('table tr', timeout=10000)
                        await page.wait_for_timeout(2000)
                        print(f"Successfully navigated to page {on_page}")
                    else:
                        print(f"Could not find button for page {on_page}")
                except Exception as e:
                    print(f"Error navigating to page {on_page}: {str(e)}")

        except Exception as e:
            print(f"[HOOK] Error during search operation: {str(e)}")

        return page

    try:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            crawler.crawler_strategy.set_hook("after_goto", after_goto)
            result = await crawler.arun(url=BASE_URL, config=crawler_config)
            
            if result and result.extracted_content:
                try:
                    properties = json.loads(result.extracted_content)
                    if not properties:
                        print("No properties found in the table.")
                        return [], detected_pages or total_pages
                    return properties, detected_pages or total_pages
                except json.JSONDecodeError:
                    print("Failed to parse JSON content")
                    return [], detected_pages or total_pages
            else:
                print("No data extracted or extraction failed.")
                return [], detected_pages or total_pages
    except Exception as e:
        print(f"An error occurred during scraping: {e}")
        return [], detected_pages or total_pages

def save_to_csv(data, filename, is_first_write=False):
    mode = "w" if is_first_write else "a"
    is_file_exists = os.path.exists(filename)
    
    with open(filename, mode, newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        if is_first_write or not is_file_exists:
            writer.writerow(["Search Term", "Account #", "Owner Name", "Situs", "DBA"])
        for row in data:
            writer.writerow([row.get("search_term"), row.get("account"), row.get("owner_name"), 
                           row.get("situs"), row.get("dba")])

def save_progress(last_completed_number, progress_file="scraping_progress.txt"):
    with open(progress_file, "w") as f:
        f.write(str(last_completed_number))

def load_progress(progress_file="scraping_progress.txt"):
    try:
        with open(progress_file, "r") as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return START_RANGE

async def process_search_term(search_term, output_filename, is_first_write):
    on_page = 1
    total_pages = 1
    all_properties = []
    
    while on_page <= total_pages:
        print(f"Searching term {search_term} - Page {on_page}/{total_pages}")
        properties, detected_pages = await scrape_properties(str(search_term), on_page, total_pages)
        
        if on_page == 1 and detected_pages > total_pages:
            total_pages = detected_pages
            print(f"Found {total_pages} pages for search term {search_term}")
        
        if properties:
            # Add search term to each property record
            for prop in properties:
                prop["search_term"] = search_term
            all_properties.extend(properties)
            print(f"Found {len(properties)} properties for search term {search_term} on page {on_page}")
            on_page += 1
        else:
            print(f"No properties found for search term {search_term} on page {on_page}")
            break
    
    if all_properties:
        save_to_csv(all_properties, output_filename, is_first_write)
        print(f"Saved {len(all_properties)} properties for search term {search_term}")
    
    return len(all_properties)


async def process_batch(batch, output_filename, is_first_write):
    """Processes a batch of search terms concurrently."""
    semaphore = asyncio.Semaphore(CONCURRENT_TASKS_LIMIT)
    tasks = []

    for search_term in batch:
        task = asyncio.create_task(process_single_term(semaphore, search_term, output_filename, is_first_write))
        tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)
    success_count = sum(1 for result in results if isinstance(result, int) and result > 0)
    print(f"Batch processed: {len(batch)} terms, successful: {success_count}, errors: {len(batch) - success_count}")

async def process_single_term(semaphore, search_term, output_filename, is_first_write):
    """Processes a single search term with a semaphore to control concurrency."""
    async with semaphore:
        try:
            return await process_search_term(search_term, output_filename, is_first_write)
        except Exception as e:
            print(f"Error processing term {search_term}: {e}")
            return 0

async def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"properties_full_{timestamp}.csv"
    progress_file = "scraping_progress.txt"

    # Load previous progress if exists
    current_number = load_progress(progress_file)
    print(f"Starting from search term: {current_number}")

    total_properties = 0
    is_first_write = True

    try:
        while current_number <= END_RANGE:
            # Create a batch of 1000 search terms
            batch = range(current_number, min(current_number + BATCH_SIZE, END_RANGE + 1))
            await process_batch(batch, output_filename, is_first_write)

            # Update progress
            current_number += BATCH_SIZE
            save_progress(current_number - 1, progress_file)
            print(f"Progress saved at search term {current_number - 1}")

            is_first_write = False  # Only the first batch should write headers

    except Exception as e:
        print(f"Error occurred: {e}")
        save_progress(current_number - 1, progress_file)
        raise

    print(f"Scraping completed! Total properties found: {total_properties}")
    print(f"Results saved to: {output_filename}")

if __name__ == "__main__":
    asyncio.run(main())
