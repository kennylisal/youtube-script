import os
import json
import mal_script
import urllib.parse
from typing import Literal
class ErrorSolver():
    def __init__(self, main_error_path, client_error_path, error_log_path) -> None:
        self.main_error_path = main_error_path
        self.client_error_path = client_error_path
        self.error_log_path= error_log_path

        self.client_errors = self.load_data_from_file(client_error_path)
        self.main_error_path = self.load_data_from_file(main_error_path)
        self.error_logs = self.load_data_from_file(error_log_path, must_exist=False)
        self.results = {}
        self.new_errors = []

    def load_data_from_file(self, file_path:str, must_exist = True):
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        if must_exist :
            raise FileNotFoundError(f"Data file '{file_path}' not found.")
        else:
            return []
        
    # fokus bikin ini saja dulu, soalnya main masih belum jelas bgmn
    async def solve_client_seasons_error(self):
        for error in self.client_errors:
            year,season = self.get_season_and_year(error)
            if year  not in self.results:
                self.results[year] = {}
            self.results[year][season] 
    
    async def resolve_error(self,type:Literal['path','paginate'], url = '',path=''):
        if type == 'paginate' : 
            return await mal_script.get_jikan

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

