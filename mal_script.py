import aiohttp
from urllib.parse import urlunparse, urlencode
import asyncio
from colorama import init, Fore, Back, Style
import os
import json
import random
from typing import Literal
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
    
    async def get_jikan_moe(self, path: str, params: dict = {}, max_attemps = 3):
        api_url = self.generate_url(path, params)
        async with self.semaphore:
            for attempt in range(max_attemps+1):
                try:
                    # if random.randint(0,6) == 3:
                    #     break
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
                    line_print(Back.RED, f"API Error at path '{path}' attemp {attempt+1}/{max_attemps + 1}")
                except Exception as e:
                    line_print(Back.RED, f"API Error at path '{path}' attemp {attempt + 1}/{max_attemps + 1}")
                
                await asyncio.sleep(3)
        msg = f"Exhausted {max_attemps + 1} attempts for {api_url} without success"
        self.raise_error(error=APIClientError(url=api_url,error=RuntimeError(msg),type='paginate'))

    def filter_wanted_attributes(self,anime_list : list[dict],filter_keys:list[str] = ["mal_id","url","approved","title","title_english","aired","rating","season","year","broadcast","studios","genres","explicit_genres","themes","demographics","score","scored_by"]):
        anime_data = [{k: anime.get(k) for k in filter_keys if k in anime} for anime in anime_list ]
        return anime_data

    # this is for get all the data from a path
    # this is used to get all the paginated data
    async def get_path_entire_data(self, path : str, ):
        try:
            # if random.randint(0,4) == 1:
            #     raise APIClientError(error=Exception("Artificial error by me"),type="path",path=path)
            first_data = await self.get_jikan_moe(path, params={'page' : 1, 'filter' : 'tv'})
            pagination = first_data.get('pagination') # type: ignore
            if pagination is None:
                raise Exception({"error" : f"{path} data is corrupt, no pagination data", "path": path})
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
                    tasks.append(self.get_jikan_moe(path,params={'page':current_index,'filter' : 'tv'}))
                results = await asyncio.gather(*tasks, return_exceptions=True)
                # 
                for result in results:
                    if not isinstance(result,Exception) and result is not None:
                        anime_data :list= result.get('data') # type: ignore
                        final_result.extend(anime_data)
            print(Fore.GREEN + f"{path} -> {len(final_result)} datas")
            processed_data = self.filter_wanted_attributes(final_result)
            return pagination,processed_data
        except Exception as e:
            if isinstance(e,APIClientError):
                self.raise_error(error=e)
            else:
                e = APIClientError(path=path,error=e,type='path')
                self.raise_error(e)
                # self.raise_error(error=APIClientError(url=self.generate_url(path),error=e,type='path'))

    
    async def add_data_to_final_result(self, season_year:int, season:str):
        year = str(season_year)
        api_path = f"/seasons/{year}/{season}"
        try:
            if year not in self.results:
                self.results[year] = {}
            pagination_info,path_data = await self.get_path_entire_data(api_path)
            self.results[year][season] = {"data" : path_data, "pagination_info" : pagination_info}
        except Exception as e:
            if not isinstance(e,APIClientError):
                e = APIClientError(path=api_path,error=e,type='path')
                self.raise_error(error=e)

    async def get_years_of_seasonal_data_v2(self, year_start = 2000, year_end = 2025):
        tasks = []
        seasons = ['winter', 'spring', 'summer', 'fall']
        for year in range(year_start,year_end+1):
            for season in seasons:
                new_task = self.add_data_to_final_result(year,season)
                tasks.append(new_task)
        await asyncio.gather(*tasks, return_exceptions=True)
        return self.results

    def print_error(self):
        line_print(Back.RED, "Here are the errors during data gathering")
        if len(self.errors) == 0:  # Or: if len(self.errors) == 0:
            print(Fore.RED + "No Error Occurred")  # Fixed spelling too
        else:
            print(Fore.RED + f"{len(self.errors)} occured")

    def raise_error(self, error:Exception):
        error_msg = str(error)
        line_print(Back.RED , error_msg)
        if isinstance(error, APIClientError):
            self.errors.append(error._get_json_format())
        raise error
    
    def clear_error(self):
        print("re-fetch all the rror path")


class JikanDataProcessor:
    def __init__(self, base_data : dict[str,dict[str,dict]]):
        self.base_data = base_data
        self.isekai_data = {}

    def gather_isekai_data(self):
        # print(self.base_data["2025"]["spring"]["data"][0])
        for year in self.base_data:
            for season in self.base_data[year]:
                anime_list : list = self.base_data[year][season].get("data",[])
                for anime in anime_list:
                    anime_theme_dict = anime.get('themes','x')
                    is_isekai = self.check_if_themes_contain_isekai(anime_theme_dict)
                    if is_isekai:
                        print(anime.get("title","pusing"))
    
    def check_if_themes_contain_isekai(self, themes:list[dict]):
        for theme in themes:
            theme_name = theme.get('name','x')
            if theme_name == "Isekai":
                return True
        return False



class APIClientError(Exception):
    def __init__(self, error: Exception, type : Literal["paginate","path"],url: str = "",path:str = ""):
        self.url = url
        self.error = error  
        self.__cause__ = error 
        self.type = type
        self.path = path
        
        # Fixed: Derive message from the passed error instance (no 'details' needed)
        error_message = str(error) if error else "Unknown error" 
        super_message = f"API Error at path '{url}': {error_message}"
        super().__init__(super_message)
    
    def _get_json_format(self):
        return {
            "path" : self.path,
            "error" : str(self.error),
            "type" : self.type,
            "url" : self.url
        }



def save_data_to_file(data, file_path:str = "data.txt"):
    with open(file_path,'w', encoding='utf-8') as file:
        json.dump(data,file,ensure_ascii=False, indent=4)

def load_data_from_file(file_path:str = "jikan.txt"):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        raise FileNotFoundError(f"Data file '{file_path}' not found.")

async def main():
    jikan_data_path = "jikan.txt"
    async with AsyncAPIClient("api.jikan.moe/v4",headers={}) as client:
        if os.path.exists(jikan_data_path):
            print(Fore.YELLOW + f"Loading data from {jikan_data_path}")
            # 
            seasonal_data = load_data_from_file(jikan_data_path)
        else:
            print(Fore.YELLOW + "Gathering data from Jikan.moe")
            # 
            seasonal_data = await client.get_years_of_seasonal_data_v2(2023,2025)
            client.print_error()
            save_data_to_file(seasonal_data,jikan_data_path)
            save_data_to_file(client.errors, "jikan_error.txt")
        # data_processor = JikanDataProcessor(seasonal_data)
        # data_processor.gather_isekai_data()

if __name__ == "__main__":
    asyncio.run(main())