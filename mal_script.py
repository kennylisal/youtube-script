import aiohttp
from urllib.parse import urlunparse, urlencode
import asyncio
from colorama import init, Fore, Back, Style
import itertools
init(autoreset=True)
def line_print(bg_color, text, end='\n\n'):
    print(bg_color + Style.BRIGHT + text, end=end)
class AsyncAPIClient:
    def __init__(self, base_url:str, headers : dict = {}, max_concurrency = 3):
        self.base_url = base_url.rstrip('/')
        self.headers = headers
        self.errors = []
        self.max_concurrency = max_concurrency
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.results = []
    
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
    
    async def _make_request(self,method, endpoint:str, params : dict):
        url = self.generate_url(path=endpoint, query_dict=params)
        print(url)
        try:
            async with self.session.request(method,url) as response:
                response.raise_for_status()
                return response
        except aiohttp.ClientError as e:
            print(f"request failed : {e}")
            raise e
    
    async def get(self, path: str, params: dict = {}):
        api_url = self.generate_url(path, params)
        try:
            async with self.semaphore:
                async with self.session.get(api_url) as res:
                    data =  await res.json()
                    line_print(Back.GREEN,f"Success fetch data from {api_url}")
                    return data
        except aiohttp.ClientError as e:
            line_print(Back.RED , f"aiohttp client error GET {api_url}")
            self.errors.append(e)
        except Exception as e:
            line_print(Back.RED , f"Error Occured Request GET {api_url}")
            self.errors.append(e)

    def _gather_data(self,datas:list):
        self.results.extend(datas)

    # this is for get all the data from a path
    # this is used to get all the paginated data
    async def get_path_entire_data(self, path : str, params : dict = {}):
        try:
            first_data = await self.get(path)
            pagination = first_data.get('pagination') # type: ignore
            page_count = pagination['last_visible_page']
            if(page_count > 1):
                tasks = []
                for i in range(1,page_count):
                    current_index = i + 1
                    tasks.append(self.get(path,params={'page':current_index}))
                results = await asyncio.gather(*tasks, return_exceptions=True)
                # 
                anime_data :list= first_data.get('data') # type: ignore
                
                self._gather_data(anime_data)
                for result in results:
                    anime_data :list= result.get('data') # type: ignore
                    if anime_data is not None:
                        
                        self._gather_data(anime_data)
            return self.results
        except Exception as e:
            raise e


async def main():
    async with AsyncAPIClient("api.jikan.moe/v4",headers={}) as client:
        results = await client.get_path_entire_data('seasons/now')
        print(len(results))

if __name__ == "__main__":
    asyncio.run(main())

