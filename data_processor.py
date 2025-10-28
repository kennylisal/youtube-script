from datetime import datetime
from typing import Literal
import json

def prepare_list_of_tuples(season_data : dict, season_id):
    anime_list : list[dict] = season_data['data']
    anime_tuple = []
    anime_explicit_relation = []
    anime_themes_relation = []
    anime_genres_relation = []
    anime_demographics_relation = []

    for anime in anime_list:
        anime_tuple.append(extract_anime_data(anime,season_id))
        anime_explicit_relation.extend(extract_anime_genres(anime,'explicit_genres'))
        anime_themes_relation.extend(extract_anime_genres(anime,'themes'))
        anime_genres_relation.extend(extract_anime_genres(anime,'genres'))
        anime_demographics_relation.extend(extract_anime_genres(anime,"demographics"))
    
    return anime_tuple, anime_explicit_relation, anime_themes_relation, anime_genres_relation, anime_demographics_relation

def extract_anime_data( anime:dict, season_id):
    data = (
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
        anime.get('scored_by', None),
        anime.get('source', None)
    )
    return data

def extract_anime_genres( anime:dict, genre_type : Literal['genres','demographics','themes','explicit_genres']):
    anime_id = anime.get('mal_id', None)
    genre_list : list[dict] = anime[genre_type]
    relation_list = []
    for genre in genre_list:
        relation_list.append((
            anime_id, genre['mal_id']
        ))
    return relation_list


# class DataProcessor:
#     def __init__(self, max_concurrency = 5):
#         now = datetime.now()
#         unix_timestamp_dt = now.timestamp()
#         # 
#         self.time_stamp = unix_timestamp_dt
#         self.error_logs = {}
#         self.semaphore = asyncio.Semaphore(max_concurrency)
    
#     # bikin banyak taska
#     # pecahkan jadi
#     # data anime yang mau di-insert
#     # data genre yang mau di insert
#     #   ingat ini data genre 4 jenis
#     #   bikin modular
#     # bikin error-nya terpecah berdasarkan operasi yang mau dibikin

# class DataProcessorError(Exception):
#     def __init__(self, error:Exception, anime_data, error_type:Literal['anime','genre','sql']):
#         self.error = error
#         self.anime_data = anime_data
#         self.error_type = error_type

#     def _get_json_format(self):
#         return {
#             "error" : self.error,
#             "anime_data" : self.anime_data,
#             "error_type" : self.error_type
#         }