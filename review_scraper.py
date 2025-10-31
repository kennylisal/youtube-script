from bs4 import BeautifulSoup
# from playwright.sync_api import sync_playwright, Page, expect
import db_handler
import asyncio
from playwright.async_api import async_playwright, Page, expect
import utils
# import random
semaphore = asyncio.Semaphore(4)

async def get_review_stats(page: Page, reviews_url, mal_id) -> dict:
    try:
        await page.goto(reviews_url, timeout=13500)  # Increased timeout, wait for idle
        
        # Wait for selector with increased timeout; verify this selector in browser dev tools
        await page.wait_for_selector(
            selector='#content > table > tbody > tr > td:nth-child(2) > div.rightside.js-scrollfix-bottom-rel > table > tbody > tr:nth-child(4) > td > div.anime-info-review__header.mal-navbar',
            timeout=13500
        )
        soup = BeautifulSoup(await page.content(), 'lxml')
        target = soup.select('#content > table > tbody > tr > td:nth-child(2) > div.rightside.js-scrollfix-bottom-rel > table > tbody > tr:nth-child(4) > td > div.anime-info-review__header.mal-navbar')
        strong_numbers = target[0].find_all('strong')
        data =  {
            'mal_id' : mal_id,
            'Recommended': int(strong_numbers[0].get_text(strip=True)),
            'Mixed Feelings': int(strong_numbers[1].get_text(strip=True)),
            'Not Recommended': int(strong_numbers[2].get_text(strip=True)),
            'Total': int(strong_numbers[3].get_text(strip=True)),
        }
        await asyncio.sleep(10)
        return data
    except Exception as e:
        e = Exception(f"Tidak ditemukan reviewnya untuk {reviews_url}")
        print(f"Error: {e}")
        raise e
        # print(page.content())  # Dump page content for debugging

async def scrape_page_review(url, mal_id):
    async with semaphore:
        for i in range(3):
            # if random.randint(0,3) == 2:
            #     break
            browser = None
            try:
                async with async_playwright() as p:
                    # Launch browser with User-Agent and headers to avoid bot detection
                    browser = await p.chromium.launch(headless=True)  # Visible for debugging
                    context = await browser.new_context(
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                        extra_http_headers={
                            'Accept-Language': 'en-US,en;q=0.9',
                            'Referer': 'https://www.google.com/'  # Optional: Fake referrer
                        }
                    )
                    page = await context.new_page()
                    
                    data = await get_review_stats(page=page, reviews_url=url,mal_id=mal_id)
                    await browser.close()
                    utils.line_print_success(f"Success scrape review on {url}")
                    return data
            except Exception as e:
                if browser:
                    await browser.close()
                utils.line_print_warning(f"attemp {i+1} / {3} failed, cause :{e}")
                await asyncio.sleep(3)

        msg = f"attempt on {url} exceed max try"
        utils.line_print_error(f"attempt on {url} exceed max try")
        return {
            'type' : 'error',
            "message" : msg,
            'url':url
        }


async def create_review_list(list_path = 'top_750_popularity.txt'):
    utils.line_print_announcement("Starting to scrape top anime review")
    errors = []
    anime_list : list[dict] = db_handler.load_data_from_file(list_path)
    tasks = []
    # anime_list_x = anime_list[0:10]
    for anime in anime_list:
        url = anime.get('url', None)
        mal_id = anime.get('mal_id', None)
        if not url:
            errors.append({
                'error' : f"Exception raised on {anime.get('url',"No url Data")}",
                'url' : url
            })
        tasks.append(scrape_page_review(url, mal_id))
    results = await asyncio.gather(*tasks,return_exceptions=True)
    print(errors)
    db_handler.save_data_to_file(results, 'review_top_750.txt')

if __name__ == "__main__":
    asyncio.run(create_review_list())
    # x = asyncio.run(scrape_page_review("https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood"))
    # print(x)