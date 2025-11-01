from typing import Literal
import aiosqlite
import os
import json
# import asyncio
# bikin check jika db ada tidak
# bikin insert many
# bikin update
from colorama import init, Fore, Back, Style


import data_processor
init(autoreset=True)
def line_print(bg_color, text, end='\n\n'):
    print(bg_color + Style.BRIGHT + text, end=end)
DB_PATH = "anime.db"

class DBHandler:  # Assuming a class context
    def __init__(self, DB_PATH = "anime.db"):
        self.DB_PATH = DB_PATH
        self.conn = None

    async def make_connection(self):
        self.conn = await aiosqlite.connect(self.DB_PATH)
        # Optional: Enable WAL for better concurrency
        cursor = await self.conn.cursor()
        await cursor.execute("PRAGMA journal_mode=WAL")
        await self.conn.commit()
        return self.conn

    async def close(self):
        if self.conn:
            await self.conn.close()

def check_db_exist():
    return os.path.exists(DB_PATH)

async def init_db(conn:aiosqlite.Connection):
    cursor = await conn.cursor()
    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS seasons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            season TEXT NOT NULL,
            UNIQUE(year, season)
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS anime (
            mal_id INTEGER PRIMARY KEY,
            season_id INTEGER NOT NULL,
            url TEXT,
            approved INTEGER,
            title TEXT,
            title_english TEXT,
            title_japanese TEXT,
            aired_json TEXT,
            rating TEXT,
            season TEXT,
            year INTEGER,
            broadcast_json TEXT,
            studios_json TEXT,
            genres_json TEXT,
            explicit_genres_json TEXT,
            themes_json TEXT,
            demographics_json TEXT,
            score REAL,
            scored_by INTEGER,
            source TEXT,
            members INTEGER,
            favorites INTEGER,
            type TEXT,
            FOREIGN KEY(season_id) REFERENCES seasons(id)
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS themes (
            mal_id INTEGER PRIMARY KEY,
            name TEXT,
            url TEXT,
            count INTEGER
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS genres (
            mal_id INTEGER PRIMARY KEY,
            name TEXT,
            url TEXT,
            count INTEGER
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS demographics (
            mal_id INTEGER PRIMARY KEY,
            name TEXT,
            url TEXT,
            count INTEGER
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS explicit_genres (
            mal_id INTEGER PRIMARY KEY,
            name TEXT,
            url TEXT,
            count INTEGER
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS anime_demographics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id INTEGER,
            demographic_id INTEGER,
            FOREIGN KEY(anime_id) REFERENCES anime(mal_id),
            FOREIGN KEY(demographic_id) REFERENCES demographics(mal_id),
            UNIQUE(anime_id,demographic_id)
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS anime_themes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id INTEGER,
            theme_id INTEGER,
            FOREIGN KEY(anime_id) REFERENCES anime(mal_id),
            FOREIGN KEY(theme_id) REFERENCES themes(mal_id),
            UNIQUE(anime_id,theme_id)
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS anime_genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id INTEGER,
            genre_id INTEGER,
            FOREIGN KEY(anime_id) REFERENCES anime(mal_id),
            FOREIGN KEY(genre_id) REFERENCES genres(mal_id),
            UNIQUE(anime_id,genre_id)
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS anime_explicit_genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id INTEGER,
            explicit_genre_id INTEGER,
            FOREIGN KEY(anime_id) REFERENCES anime(mal_id),
            FOREIGN KEY(explicit_genre_id) REFERENCES explicit_genres(mal_id),
            UNIQUE(anime_id,explicit_genre_id)
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS anime_broadcast (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id INTEGER,
            day TEXT,
            time TEXT,
            timezone TEXT,
            string_text TEXT,
            FOREIGN KEY(anime_id) REFERENCES anime(mal_id)
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS anime_studios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id INTEGER,
            studio_id INTEGER,
            type TEXT,
            name TEXT,
            url TEXT,
            FOREIGN KEY(anime_id) REFERENCES anime(mal_id)
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS anime_producers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id INTEGER,
            producer_id INTEGER,
            type TEXT,
            name TEXT,
            url TEXT,
            FOREIGN KEY(anime_id) REFERENCES anime(mal_id)
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS anime_aired_schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id INTEGER,
            aired_from DATETIME,
            aired_to DATETIME,
            string_text TEXT,
            FOREIGN KEY(anime_id) REFERENCES anime(mal_id)
        )
    ''')

    await cursor.execute('''
        CREATE TABLE IF NOT EXISTS anime_recomendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anime_id INTEGER,
            recomended INTEGER,
            mixed_feelings INTEGER,
            not_recomended INTEGER,
            total INTEGER
        )
    ''')
    
    await cursor.execute('CREATE INDEX IF NOT EXISTS idx_year_season ON seasons(year, season)')
    await cursor.execute('CREATE INDEX IF NOT EXISTS idx_anime_season ON anime(season_id)')

    await conn.commit()


async def insert_from_dict(conn:aiosqlite.Connection,results: dict, year_x:int):
    Errors = []
    try:
        cursor = await conn.cursor()
        for year_str, year_data in results.items():
            year = int(year_str)
            for season, season_data in year_data.items():
                try:
                    season_id = await set_anime_season(cursor,season,year)
                    anime_tuples, anime_explicit_relation, anime_themes_relation, anime_genres_relation, anime_demographics_relation,anime_broadcast,anime_studios,anime_producers,anime_air_data = data_processor.prepare_list_of_tuples(season_data,season_id)
                    await anime_bulk_insertion(cursor, anime_tuples)
                    await anime_genres_relation_bulk_insertion(cursor,anime_explicit_relation,'anime_explicit_genres')
                    await anime_genres_relation_bulk_insertion(cursor,anime_themes_relation,'anime_themes')
                    await anime_genres_relation_bulk_insertion(cursor, anime_genres_relation,'anime_genres')
                    await anime_genres_relation_bulk_insertion(cursor, anime_demographics_relation, 'anime_demographics')
                    await anime_broadcast_insert_bulk(cursor, anime_broadcast)
                    await anime_studio_insert_bulk(cursor, anime_studios)
                    await anime_producer_insert_bulk(cursor, anime_producers)
                    await anime_aired_insert_bulk(cursor,anime_air_data)
                except Exception as e:
                    if not isinstance(e,DBHandlerError):
                        e = DBHandlerError(e, season_data)
                    print(e)
                    Errors.append(e._get_json_format())
            line_print(Back.BLUE,f"Success Storing {year} Anime Data")
        await conn.commit()
    except Exception as e:
        line_print(Back.RED,f"Failed Storing {year_x} Anime Data, cause : {e}")
        if conn:
            await conn.rollback()
        raise e  
    finally:
        return Errors


async def set_anime_season(cursor:aiosqlite.Cursor,season :str,year:int):
    try:
        await cursor.execute('INSERT OR IGNORE INTO seasons (year, season) VALUES (?, ?)', (year, season))
        await cursor.execute('SELECT id FROM seasons WHERE year = ? AND season = ?', (year, season))
        season_id = await cursor.fetchone()
        
        if season_id is None:
            raise ValueError(f"No season found for year={year}, season={season}")
        return season_id[0]
    except Exception as e :
        print(e)
        raise e

async def anime_bulk_insertion(cursor:aiosqlite.Cursor, anime_tuples):
    try:
        await cursor.executemany('''
            INSERT OR IGNORE INTO anime (
                mal_id, season_id, url, approved, title, title_english, title_japanese, aired_json,
                rating, season, year, broadcast_json, studios_json, genres_json,
                explicit_genres_json, themes_json, demographics_json, score, scored_by,source, members, favorites, type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?)
        ''', anime_tuples)
    except Exception as e:
        print(e)
        raise e
    


async def anime_genres_relation_bulk_insertion(cursor:aiosqlite.Cursor,relation_tuples,genre_type: Literal['anime_genres','anime_demographics','anime_themes','anime_explicit_genres']):
    column_name = ''
    if genre_type == 'anime_demographics':
        column_name = 'demographic_id'
    elif genre_type == 'anime_explicit_genres':
        column_name = 'explicit_genre_id'
    elif genre_type == 'anime_genres':
        column_name = 'genre_id'
    elif genre_type == 'anime_themes':
        column_name = 'theme_id'
    try:
        await cursor.executemany(f'''
            INSERT OR IGNORE INTO {genre_type} (
                anime_id, {column_name}
            )
            VALUES (?, ?)
        ''', relation_tuples)
    except Exception as e:
        print(e)
        raise e

async def genres_insert_bulk(cursor:aiosqlite.Cursor,genre_data:list[dict], genre_choice:str):
    try:
        anime_tuples = []
        for genre in genre_data:
            anime_tuples.append((
                genre.get('mal_id'),
                genre.get('name'),
                genre.get('url'),
                genre.get('count')
            ))
        
        # Bulk insert anime for this season
        await cursor.executemany(f'''
            INSERT OR IGNORE INTO {genre_choice} (
                mal_id, name,url,count
            )
            VALUES (?, ?, ?, ?)
        ''', anime_tuples)
    except Exception as e:
        print(e)
        raise e

async def anime_broadcast_insert_bulk(cursor:aiosqlite.Cursor,data:list):
    try:
        # Bulk insert anime for this season
        await cursor.executemany(f'''
            INSERT OR IGNORE INTO anime_broadcast (
                anime_id,day,time,timezone,string_text
            )
            VALUES (?, ?, ?, ?,?)
        ''', data)
    except Exception as e:
        print(e)
        print("gagal insert anime broadcast")
        raise e

async def anime_studio_insert_bulk(cursor:aiosqlite.Cursor,data:list):
    try:
        # Bulk insert anime for this season
        await cursor.executemany(f'''
            INSERT OR IGNORE INTO anime_studios (
                anime_id,studio_id,type,name,url
            )
            VALUES (?, ?, ?, ?,?)
        ''', data)
    except Exception as e:
        print(e)
        print("gagal insert anime studio")
        raise e

async def anime_insert_top_recomendation(cursor:aiosqlite.Cursor,data:list):
    try:
        # Bulk insert anime for this season
        await cursor.executemany(f'''
            INSERT OR IGNORE INTO anime_recomendations (
                anime_id,recomended,mixed_feelings,not_recomended,total
            )
            VALUES (?, ?, ?, ?, ?)
        ''', data)
    except Exception as e:
        print(e)
        print("gagal insert recomendtaion")
        raise e
    
async def anime_producer_insert_bulk(cursor:aiosqlite.Cursor,data:list):
    try:
        # Bulk insert anime for this season
        await cursor.executemany(f'''
            INSERT OR IGNORE INTO anime_producers (
                anime_id,producer_id,type,name,url
            )
            VALUES (?, ?, ?, ?, ?)
        ''', data)
    except Exception as e:
        print(e)
        print("gagal insert anime producer")
        raise e

async def anime_aired_insert_bulk(cursor:aiosqlite.Cursor,data:list):
    try:
        # Bulk insert anime for this season
        await cursor.executemany(f'''
            INSERT OR IGNORE INTO anime_aired_schedule (
                anime_id, aired_from, aired_to, string_text
            )
            VALUES (?, ?, ?, ?)
        ''', data)
    except Exception as e:
        print(e)
        print("gagal insert anime aired")
        raise e

async def insert_genre_from_dict(conn : aiosqlite.Connection,results: dict[str,list[dict]], genre_choice:str):
    Errors = []
    try:
        cursor = await conn.cursor()
        dict_data = results.get('data',[])
        await genres_insert_bulk(cursor,dict_data, genre_choice)
        await conn.commit()
    except aiosqlite.Error as e:
        if conn:
            await conn.rollback()
        raise e  
    except Exception as e:
        if conn:
            await conn.rollback()
        raise e
    finally:
        return Errors

async def insert_recomendation_data(conn : aiosqlite.Connection,data:list[tuple]):
    Errors = []
    try:
        cursor = await conn.cursor()
        await anime_insert_top_recomendation(cursor,data)
        await conn.commit()
    except aiosqlite.Error as e:
        if conn:
            await conn.rollback()
        raise e  
    except Exception as e:
        if conn:
            await conn.rollback()
        raise e
    finally:
        return Errors

async def test_query():
    conn = await aiosqlite.connect(DB_PATH)
    cursor = await conn.cursor()

    await cursor.execute("select * from anime order by RANDOM() limit 20")
    results = await cursor.fetchall()

    for result in results:
        print(result)
    # print(cursor.execute("select * from seasons order by RANDOM() limit 20").fetchall())

def save_data_to_file(data, file_path: str = "data.txt", clear_file=False):
    
    if clear_file:
        open(file_path, 'w').close()  
    
    # Then write the new data
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

def load_data_from_file(file_path:str = "jikan.txt"):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        raise Exception("No such path")
        
class DBHandlerError(Exception):
    def __init__(self, error: Exception, data):
        self.error = error  
        self.__cause__ = error 
        self.data = data

        super_message = f"DB handler Error"
        self.message = super_message
        super().__init__(super_message)
    
    def _get_json_format(self):
        return {
            "error" : str(self.error),
            "type" : "DB_HANDLER",
            "data" : self.data
        }