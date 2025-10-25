import sqlite3
import os
import json
# bikin check jika db ada tidak
# bikin insert many
# bikin update

DB_PATH = "anime.db"

def check_db_exist():
    return os.path.exists(DB_PATH)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS seasons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            season TEXT NOT NULL,
            UNIQUE(year, season)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS anime (
            mal_id INTEGER PRIMARY KEY,
            season_id INTEGER NOT NULL,
            url TEXT,
            approved INTEGER,
            title TEXT,
            title_english TEXT,
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
            FOREIGN KEY(season_id) REFERENCES seasons(id)
        )
    ''')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_year_season ON seasons(year, season)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_anime_season ON anime(season_id)')
    
    conn.commit()
    conn.close()


def insert_from_dict(results: dict):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        for year_str, year_data in results.items():
            year = int(year_str)
            for season, season_data in year_data.items():
                # Insert or ignore season, get ID
                cursor.execute('INSERT OR IGNORE INTO seasons (year, season) VALUES (?, ?)', (year, season))
                cursor.execute('SELECT id FROM seasons WHERE year = ? AND season = ?', (year, season))
                season_id = cursor.fetchone()[0]
                
                anime_list = season_data.get('data', [])
                anime_tuples = []
                for anime in anime_list:
                    anime_tuples.append((
                        anime.get('mal_id', None),
                        season_id,
                        anime.get('url', None),
                        int(anime.get('approved', False)) if anime.get('approved') is not None else None,
                        anime.get('title', None),
                        anime.get('title_english', None),
                        json.dumps(anime.get('aired', {})),
                        anime.get('rating', None),
                        anime.get('season', None),
                        anime.get('year', None),
                        json.dumps(anime.get('broadcast', {})),
                        json.dumps(anime.get('studios', [])),
                        json.dumps(anime.get('genres', [])),
                        json.dumps(anime.get('explicit_genres', [])),
                        json.dumps(anime.get('themes', [])),
                        json.dumps(anime.get('demographics', [])),
                        anime.get('score', None),
                        anime.get('scored_by', None)
                    ))
                
                # Bulk insert anime for this season
                cursor.executemany('''
                    INSERT OR IGNORE INTO anime (
                        mal_id, season_id, url, approved, title, title_english, aired_json,
                        rating, season, year, broadcast_json, studios_json, genres_json,
                        explicit_genres_json, themes_json, demographics_json, score, scored_by
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', anime_tuples)
        
        conn.commit()
    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        raise e  
    finally:
        if conn:
            conn.close()

def test_query():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    results = cursor.execute("select * from anime order by RANDOM() limit 20").fetchall()
    for result in results:
        print(result)
    # print(cursor.execute("select * from seasons order by RANDOM() limit 20").fetchall())

def save_data_to_file(data, file_path:str = "data.txt"):
    with open(file_path,'w', encoding='utf-8') as file:
        json.dump(data,file,ensure_ascii=False, indent=4)

def load_data_from_file(file_path:str = "jikan.txt"):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)