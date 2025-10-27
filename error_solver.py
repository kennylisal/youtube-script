import os
import json
from mal_script import APIClientError, line_print, AsyncAPIClient
import urllib.parse
from typing import Literal
import asyncio
import aiohttp
from colorama import Back
class ErrorSolver():
    def __init__(self, main_error_path, client_error_path, error_log_path, anime_type,max_concurrency = 2) -> None:
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.client_errors = self.load_data_from_file(client_error_path)
        self.main_error_path = self.load_data_from_file(main_error_path)
        self.error_logs = self.load_data_from_file(error_log_path, must_exist=False)
        self.results = {}
        self.new_errors = []
        self.anime_type = anime_type
        # planning to make error log to be divided by time executed
    
    async def __aenter__(self):
        # connector = aiohttp.TCPConnector(limit=1)  # 1 concurrent connection
        self.session = aiohttp.ClientSession(headers={})
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def load_data_from_file(self, file_path:str, must_exist = True):
        final_data = []
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    final_data = json.load(f)
        except Exception as e:
            if must_exist :
                raise FileNotFoundError(f"Data file '{file_path}' not found.")
        finally:
            return final_data
        
    # fokus bikin ini saja dulu, soalnya main masih belum jelas bgmn
    async def solve_client_seasons_error(self):
        for error in self.client_errors:
            year,season = self.get_season_and_year(error)
            if year  not in self.results:
                self.results[year] = {}
            self.results[year][season] 


    
    async def get_direct_api_data_from_url(self,direct_url, anime_type):
        async with AsyncAPIClient(base_url="api.jikan.moe/v4",headers={},anime_type=anime_type) as client:
            try:
                api_data = await client.get_jikan_moe('',{},direct_url=direct_url)
                return api_data.get('data',[])
            except Exception as e:
                if isinstance(e, APIClientError):
                    self.new_errors.append(e._get_json_format())
                    raise e
                exception =  APIClientError(error=e,type="paginate")
                self.new_errors.append(exception._get_json_format())
                raise exception
    
    async def get_path_data(self,path,anime_type)->tuple[dict,list]:
        async with AsyncAPIClient(base_url="api.jikan.moe/v4",headers={},anime_type=anime_type) as client:
            try:
                path_data = await client.get_path_entire_data(path=path)
                return path_data
            except Exception as e:
                if isinstance(e, APIClientError):
                    self.new_errors.append(e._get_json_format())
                    raise e
                exception =  APIClientError(error=e,type="paginate")
                self.new_errors.append(exception._get_json_format())
                raise exception
    
    async def get_year_mal_data(self,year,anime_type):
        async with AsyncAPIClient(base_url="api.jikan.moe/v4",headers={},anime_type=anime_type) as client:
            try:
                year_data = await client.get_year_seasonal_data(year)
                return year_data
            except Exception as e:
                self.raise_error(e)

    def raise_error(self,error:Exception):
        line_print(Back.RED , str(error))
        if isinstance(error, APIClientError):
            self.new_errors.append(error._get_json_format())
            raise error
        exception =  APIClientError(error=error,type="paginate")
        self.new_errors.append(exception._get_json_format())
        raise exception
    
    async def resolve_paginate_error(self, error:dict[str,any]): # type: ignore
        try:

            url = error.get('url')
            year,season = self.get_year_season_from_url(url)
            data = await self.get_direct_api_data_from_url(url,self.anime_type)
            print(f"success taking data from {url}")
            # Ensure year key exists
            if year not in self.results:
                self.results[year] = {}
            
            # Ensure season key exists
            if season not in self.results[year]:
                self.results[year][season] = {"data": data, "pagination_info": {}}
            else:
                self.results[year][season]['data'].extend(data)
            
            print(f"Bottom check")
        except Exception as e:
            # print(f"error on resolving paginate {error.get('url')}, cause : {e}")
            self.new_errors.append({
                "url" : error.get('url'),
                "message" : f"error on resolving paginate {error.get('url')}",
                "type" : "paginate",
                "error" : e
            })
            # raise e
        
    async def resolve_path_error(self, error:dict[str,any]): # type: ignore
        try:
            path = error.get('path')
            year,season = self.get_year_season_from_path(path)
            pagination, data =  await self.get_path_data(path,self.anime_type)
            self.results.setdefault(year, {})[season] = {"data": data, "pagination_info": pagination}
        except Exception as e:
            # if not isinstance(e,APIClientError):
            #     line_print(Back.RED, str(e))
            self.new_errors.append({
                "path" : error.get('url'),
                "message" : f"error on resolving paginate {error.get('path')}",
                "type" : "path",
                "error" : e
            })
            # raise e

    # main function
    async def resolve_client_errors(self):
        tasks = []
        for error in self.client_errors:
            # print(error)
            if error.get('type') == 'paginate' :
                tasks.append(self.resolve_paginate_error(error))
            else:
                tasks.append(self.resolve_path_error(error))
        await asyncio.gather(*tasks,return_exceptions=True)
        return self.results
            
    def get_year_season_from_url(self, url):
        parsed_url = urllib.parse.urlparse(url)
        path = parsed_url.path
        parts = path.split('/')
        year = int(parts[3])
        season = parts[4]
        return year,season

    def get_year_season_from_path(self,path):
        parts = path.split('/')
        year = int(parts[2])
        season = parts[3]
        return year,season

    def get_season_and_year(self,error_instance:dict):
        if error_instance.get('type') == "path":
            path : str = error_instance.get('path') # type: ignore
            parts = path.split('/')
            year = int(parts[2])
            season = parts[3]
            return year,season
        else:
            url = error_instance.get('url','')
            parsed_url = urllib.parse.urlparse(url)
            path = parsed_url.path
            parts = path.split('/')
            year = int(parts[3])
            season = parts[4]
            return year,season