import asyncio
import mal_script
from db_handler import DBHandler, insert_from_dict, save_data_to_file, init_db, insert_genre_from_dict
from typing import Literal
from error_solver import ErrorSolver
from colorama import Back

MAIN_ERROR_PATH = "MAIN_ERROR.txt"
API_CLIENT_ERROR_PATH = "API_CLIENT_ERROR.txt"
ERROR_SOLVER_LOG_PATH = "error_solver_log.txt"

async def get_year_seasonal_data(year:int, anime_type:Literal['tv', 'ova', 'ona','movie']):
    async with mal_script.AsyncAPIClient(base_url="api.jikan.moe/v4",headers={},anime_type=anime_type) as client:
        year_data = await client.get_year_seasonal_data(year)
        return year_data,client.errors

async def run_error_resolver():
    async with ErrorSolver(anime_type='tv',client_error_path=API_CLIENT_ERROR_PATH,main_error_path=MAIN_ERROR_PATH,error_log_path=ERROR_SOLVER_LOG_PATH) as solver:
        # await db_handler.init_db()
        db_handler =  DBHandler()
        db_connection = await db_handler.make_connection()
        re_fetched_data = await solver.resolve_client_errors()
        # print(re_fetched_data)
        db_error_list = await insert_from_dict(db_connection,re_fetched_data)
        # writing error to logs
        solver.new_errors.extend(db_error_list)
        save_data_to_file(solver.new_errors, ERROR_SOLVER_LOG_PATH, clear_file=True)
        await db_handler.close()

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

async def main():
    MAX_YEAR = 2025 + 1
    MIN_YEAR = 2015
    ANIME_TYPE = 'tv'
    
    main_errors = []
    client_errors = []
    db_handler =  DBHandler()
    db_connection = await db_handler.make_connection()
    await init_db(db_connection)

    for year in range(MIN_YEAR,MAX_YEAR):
        data = None
        try:
            data,errors = await get_year_seasonal_data(year=year,anime_type=ANIME_TYPE)
            await insert_from_dict(db_connection, data)
            client_errors.extend(errors)
        except Exception as e:
            # this is only to catch sqllite write error
            main_errors.append(MainError(e, year)._get_json_format())
            if data is not None:
                file_path = f"jikan/{year}"
                save_data_to_file(data, file_path)

    # db_handler.test_query()
    save_data_to_file(main_errors,MAIN_ERROR_PATH)
    save_data_to_file(client_errors,API_CLIENT_ERROR_PATH)
    mal_script.line_print(Back.BLUE, f"Done Gathering Data from {MIN_YEAR} to {MAX_YEAR}")

async def initiate_and_gather_jikan_genres():
    mal_script.line_print(Back.BLUE, "Initiating all MAL Genres Gathering")
    db_handler =  DBHandler()
    db_connection = await db_handler.make_connection()
    await init_db(db_connection)
    genre_filters = ['genres','explicit_genres','themes','demographics']
    for genre in genre_filters:
        genre_dict = await mal_script.gather_jikan_genres_data(genre)
        await insert_genre_from_dict(db_connection,genre_dict,genre)
    mal_script.line_print(Back.BLUE, "Done Gathering all MAL Genres")

if __name__ == "__main__":
    # asyncio.run(run_error_resolver())
    asyncio.run(initiate_and_gather_jikan_genres())