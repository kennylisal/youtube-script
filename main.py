import asyncio
import mal_script
import db_handler
from typing import Literal
from error_solver import ErrorSolver

MAIN_ERROR_PATH = "MAIN_ERROR.txt"
API_CLIENT_ERROR_PATH = "API_CLIENT_ERROR.txt"
ERROR_SOLVER_LOG_PATH = "error_solver_log.txt"

async def main():
    MAX_YEAR = 2025 + 1
    MIN_YEAR = 2020

    ANIME_TYPE = 'tv'
    main_errors = []
    client_errors = []
    db_handler.init_db()
    for year in range(MIN_YEAR,MAX_YEAR):
        data = None
        try:
            data,errors = await get_year_seasonal_data(year=year,anime_type=ANIME_TYPE)
            db_handler.insert_from_dict(data)
            client_errors.extend(errors)
        except Exception as e:
            # this is only to catch sqllite write error
            main_errors.append(MainError(e, year)._get_json_format())
            if data is not None:
                file_path = f"jikan/{year}"
                db_handler.save_data_to_file(data, file_path)

    db_handler.save_data_to_file(main_errors,MAIN_ERROR_PATH)
    db_handler.save_data_to_file(client_errors,API_CLIENT_ERROR_PATH)

async def get_year_seasonal_data(year:int, anime_type:Literal['tv', 'ova', 'ona','movie']):
    async with mal_script.AsyncAPIClient(base_url="api.jikan.moe/v4",headers={},anime_type=anime_type) as client:
        year_data = await client.get_year_seasonal_data(year)
        return year_data,client.errors

async def run_error_resolver():
    async with ErrorSolver(anime_type='tv',client_error_path=API_CLIENT_ERROR_PATH,main_error_path=MAIN_ERROR_PATH,error_log_path=ERROR_SOLVER_LOG_PATH) as solver:
        re_fetched_data = await solver.resolve_client_errors()
        db_handler.insert_missing_anime_from_dict(re_fetched_data)
        db_handler.save_data_to_file(solver.new_errors, ERROR_SOLVER_LOG_PATH, clear_file=True)

class MainError(Exception):
    def __init__(self, error:Exception, year) -> None:
        self.year = str(year)
        self.error = error
        self.__cause__ = error

        super_message = f"Error when retrieving {year}"
        super().__init__(super_message)
    
    def _get_json_format(self):
        return {
            "year" : self.year,
            "error" : str(self.error)
        }

if __name__ == "__main__":
    asyncio.run(main())

    # try:
    #     if not db_handler.check_db_exist():
    #         seasonal_dict = await mal_script.main()
    #         db_handler.init_db()
    #         db_handler.insert_from_dict(seasonal_dict)
    #     db_handler.test_query()
    # except Exception as e:
    #     print(e)