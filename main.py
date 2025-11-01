import asyncio
import mal_script
from db_handler import DBHandler, insert_from_dict, save_data_to_file, init_db, insert_genre_from_dict, load_data_from_file, insert_recomendation_data
from typing import Literal
from error_solver import ErrorSolver
from colorama import Back

MAIN_ERROR_PATH = "MAIN_ERROR.txt"
API_CLIENT_ERROR_PATH = "API_CLIENT_ERROR.txt"
ERROR_SOLVER_LOG_PATH = "error_solver_log.txt"

async def get_year_seasonal_data(year:int, anime_type:Literal['tv', 'ova', 'ona','movie']):
    async with mal_script.AsyncAPIClient(base_url="api.jikan.moe/v4",headers={},anime_type=anime_type) as client:
        year_data,logs = await client.get_year_seasonal_data(year)
        return year_data,client.errors,logs

async def run_error_resolver():
    async with ErrorSolver(anime_type='tv',client_error_path=API_CLIENT_ERROR_PATH,main_error_path=MAIN_ERROR_PATH,error_log_path=ERROR_SOLVER_LOG_PATH) as solver:
        # await db_handler.init_db()
        db_handler =  DBHandler()
        db_connection = await db_handler.make_connection()
        re_fetched_data = await solver.resolve_client_errors()
        # print(re_fetched_data)
        db_error_list = await insert_from_dict(db_connection,re_fetched_data,0)
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

async def gather_seasonal_data(db_connection, part_name:str, MAX_YEAR:int, MIN_YEAR:int):
    MAX_YEAR += 1
    ANIME_TYPE = 'tv'
    mal_script.line_print(Back.BLUE, f"Starting to Gather Data from {MIN_YEAR} to {MAX_YEAR - 1}")
    main_errors = []
    client_errors = []
    operation_logs = ["Saya tes ji ini log", "bgmn jadinya ya"]
    for year in range(MIN_YEAR,MAX_YEAR):
        data = None
        try:
            data,errors,logs = await get_year_seasonal_data(year=year,anime_type=ANIME_TYPE)
            await insert_from_dict(db_connection, data, year)
            logs.append(operation_logs)
            client_errors.extend(errors)
        except Exception as e:
            # this is only to catch sqllite write error
            main_errors.append(MainError(e, year)._get_json_format())
            if data is not None:
                file_path = f"jikan/{year}"
                save_data_to_file(data, file_path)
        await asyncio.sleep(5)

    save_data_to_file(main_errors,f"{part_name}_"+MAIN_ERROR_PATH)
    save_data_to_file(client_errors,f"{part_name}_"+API_CLIENT_ERROR_PATH)
    save_data_to_file(operation_logs, "anime_gathering_logs.txt")
    mal_script.line_print(Back.BLUE, f"Done Gathering Data from {MIN_YEAR} to {MAX_YEAR}")

async def initiate_and_gather_jikan_genres(db_connection):
    mal_script.line_print(Back.BLUE, "Initiating all MAL Genres Gathering")
    # await init_db(db_connection)
    genre_filters = ['genres','explicit_genres','themes','demographics']
    for genre in genre_filters:
        genre_dict = await mal_script.gather_jikan_genres_data(genre)
        await insert_genre_from_dict(db_connection,genre_dict,genre)
    mal_script.line_print(Back.BLUE, "Done Gathering all MAL Genres")

async def insert_review_to_sqlite():
    db_handler =  DBHandler()
    db_connection = await db_handler.make_connection() 
    data = load_data_from_file("review_top_Popularity.txt")
    try:
        tuple_data = [
            (d['mal_id'], d['Recommended'], d['Mixed Feelings'], d['Not Recommended'], d['Total'])
            for d in data
        ]
    except Exception as e:
        print(e)
        raise e
    await insert_recomendation_data(db_connection,tuple_data)
    mal_script.line_print(Back.BLUE, f"Done Inserting recomendation data")
    await db_handler.close()

async def main():
    db_handler =  DBHandler()
    db_connection = await db_handler.make_connection()
    seasonal_gather_instruction = [{'name' : 'Part1', 'min_year':1917, 'max_year':1957},{'name' : 'Part2', 'min_year':1958, 'max_year':2000},{'name' : 'Part3', 'min_year':2001, 'max_year':2025}]
    # seasonal_gather_instruction = [{'name' : 'Part3', 'min_year':2001, 'max_year':2025}]
    await init_db(db_connection)
    await initiate_and_gather_jikan_genres(db_connection)
    for instruction in seasonal_gather_instruction:
        name = instruction['name']
        min_year = instruction['min_year']
        max_year = instruction['max_year']
        await gather_seasonal_data(db_connection,part_name=name,MAX_YEAR=max_year,MIN_YEAR=min_year)
        mal_script.line_print(Back.BLUE, f"{name} of gathering done")
    await db_handler.close()

if __name__ == "__main__":
    asyncio.run(insert_review_to_sqlite())