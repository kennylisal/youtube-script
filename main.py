import asyncio
import mal_script
import db_handler


async def main():
    MAX_YEAR = 2025 + 1
    MIN_YEAR = 2020
    MAIN_ERROR_PATH = "MAIN_ERROR.txt"
    API_CLIENT_ERROR_PATH = "API_CLIENT_ERROR.txt"
    ANIME_TYPE = 'tv'
    main_errors = []
    client_errors = []
    db_handler.init_db()
    for year in range(MIN_YEAR,MAX_YEAR):
        try:
            data,errors = await mal_script.get_year_seasonal_data(year=year,anime_type=ANIME_TYPE)
            db_handler.insert_from_dict(data)
            client_errors.extend(errors)
        except Exception as e:
            main_errors.append(MainError(e, year)._get_json_format())

    db_handler.save_data_to_file(main_errors,MAIN_ERROR_PATH)
    db_handler.save_data_to_file(client_errors,API_CLIENT_ERROR_PATH)


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