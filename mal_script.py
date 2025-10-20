import aiohttp
from urllib.parse import urlunparse, urlencode
import asyncio
from colorama import init, Fore, Back, Style

init(autoreset=True)
def line_print(bg_color, text, end='\n\n'):
    print(bg_color + Style.BRIGHT + text, end=end)
class AsyncAPIClient:
    def __init__(self, base_url:str, headers : dict = {}, max_concurrency = 4):
        self.base_url = base_url.rstrip('/')
        self.headers = headers
        self.errors = []
        self.max_concurrency = max_concurrency
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.results = {}
    
    async def __aenter__(self):
        # connector = aiohttp.TCPConnector(limit=1)  # 1 concurrent connection
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def generate_url(self, path='', query_dict={}):
        scheme = 'https'
        netloc = self.base_url

        new_query = urlencode(query_dict, doseq=True)

        new_url = urlunparse((scheme, netloc, path, '', new_query, ''))
        return new_url
    
    async def get(self, path: str, params: dict = {}, max_attemps = 3):
        api_url = self.generate_url(path, params)
        async with self.semaphore:
            for attempt in range(max_attemps+1):
                try:
                    async with self.session.get(api_url) as res:
                        if res.status == 200:
                            data = await res.json()
                            line_print(Back.GREEN,f"Success fetch data from {api_url}")
                            return data
                        else:
                            line_print(Back.YELLOW, f"Bad status {res.status} for {api_url}, attempt {attempt + 1}/{max_attemps + 1}")
                            if attempt == max_attemps:
                                raise aiohttp.ClientResponseError(
                                    res.request_info, res.history, status=res.status,
                                    message=f"Failed after {max_attemps + 1} attempts"
                                )
                            
                except aiohttp.ClientResponseError as e:
                    line_print(Back.RED , f"aiohttp client error GET {api_url}")
                    # self.errors.append(e)
                    raise e
                except Exception as e:
                    line_print(Back.RED , f"Error Occured Request GET {api_url}")
                    # self.errors.append(e)
                    raise e
                
                await asyncio.sleep(3)
        raise RuntimeError(f"Exhausted {max_attemps + 1} attempts for {api_url} without success")

    # this is for get all the data from a path
    # this is used to get all the paginated data
    async def get_path_entire_data(self, path : str):
        try:
            first_data = await self.get(path)
            pagination = first_data.get('pagination') # type: ignore
            if pagination is None:
                return first_data.get('data', [])
            page_count = pagination.get('last_visible_page', 1)
            # 
            final_result = []
            anime_data :list= first_data.get('data') # type: ignore
            final_result.extend(anime_data)
            # 
            if(page_count > 1):
                tasks = []
                for i in range(1,page_count):
                    current_index = i + 1
                    tasks.append(self.get(path,params={'page':current_index}))
                results = await asyncio.gather(*tasks)
                # 
                for result in results:
                    anime_data :list= result.get('data') # type: ignore
                    if anime_data is not None:
                        final_result.extend(anime_data)
            print(Fore.GREEN + f"{path} -> {len(final_result)} datas")
            return final_result
        except RuntimeError as e:
            raise e
        except Exception as e:
            raise e
    
    async def add_data_to_final_result(self, year:int, season:str):
        api_path = f"/seasons/{year}/{season}"
        try:
            if year not in self.results:
                self.results[year] = {}
            self.results[year][season] = await self.get_path_entire_data(api_path)
        except Exception as e:
            self.errors.append({"error" : e, "path" : api_path})
            raise e

    # {
    #   2025 : {
    #   'winter' : 
    #   },
    #  'summer': {}
    # }
    async def get_years_of_seasonal_data_v2(self, year_start = 2000, year_end = 2025):
        tasks = []
        seasons = ['winter', 'spring', 'summer', 'fall']
        for year in range(year_start,year_end+1):
            for season in seasons:
                new_task = self.add_data_to_final_result(year,season)
                tasks.append(new_task)
        await asyncio.gather(*tasks, return_exceptions=True)
        return self.results

class JikanDataProcessor:
    def __init__(self, base_data : list):
        self.base_data = base_data
    
    def get_yearly_isekais(self):
        print("pusing")

async def main():
    async with AsyncAPIClient("api.jikan.moe/v4",headers={}) as client:
        final_result = await client.get_years_of_seasonal_data_v2(2025,2025)
        print(final_result[2025]['fall'])
        print(client.errors)
        # coba bikin list baru yang isinya hanya theme -> isekai
        # tampilkan jumlah isekai di sebuah season / tampilkan total isekai di itu season
        # tampilkan jumlah isekai yearly / yg normal 

if __name__ == "__main__":
    asyncio.run(main())