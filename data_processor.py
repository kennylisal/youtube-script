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
    anime_broadcast = []
    anime_studios = []
    anime_producers = []
    anime_air_data = []
    

    for anime in anime_list:
        anime_tuple.append(extract_anime_data(anime,season_id))
        anime_explicit_relation.extend(extract_anime_genres(anime,'explicit_genres'))
        anime_themes_relation.extend(extract_anime_genres(anime,'themes'))
        anime_genres_relation.extend(extract_anime_genres(anime,'genres'))
        anime_demographics_relation.extend(extract_anime_genres(anime,"demographics"))
        anime_broadcast.append(extract_anime_broadcast(anime))
        anime_studios.extend(extract_anime_studio(anime))
        anime_producers.extend(extract_anime_producers(anime))
        anime_air_data.append(extract_anime_air(anime))
    
    return anime_tuple, anime_explicit_relation, anime_themes_relation, anime_genres_relation, anime_demographics_relation, anime_broadcast,anime_studios,anime_producers,anime_air_data

def extract_anime_data( anime:dict, season_id):
    data = (
        anime.get('mal_id', None),
        season_id,
        anime.get('url', None),
        int(anime.get('approved', False)) if anime.get('approved') is not None else None,
        anime.get('title', None),
        anime.get('title_english', None),
        anime.get('title_japanese', None),
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

def extract_anime_genres( anime:dict, genre_type : Literal['genres','demographics','themes','explicit_genres'])->list:
    anime_id = anime.get('mal_id', None)
    genre_list : list[dict] = anime[genre_type]
    relation_list = []
    for genre in genre_list:
        relation_list.append((
            anime_id, genre['mal_id']
        ))
    return relation_list

def extract_anime_broadcast(anime:dict) -> tuple:
    try:
        anime_id = anime.get('mal_id', None)
        broadcast_data = anime.get("broadcast", {})
        data = (
            anime_id,
            broadcast_data.get("day",None),
            broadcast_data.get("time",None),
            broadcast_data.get("timezone",None),
            broadcast_data.get("string",None),
        )
        return data
    except Exception as e:
        print(e)
        raise e

def extract_anime_studio(anime:dict) -> list:
    try:
        anime_id = anime.get('mal_id', None)
        studios : list[dict]=  anime['studios']
        anime_studios = []
        for studio in studios:
            anime_studios.append((
                anime_id,
                studio.get('mal_id',None),
                studio.get('type',None),
                studio.get('name',None),
                studio.get('url',None),
            ))
        return anime_studios
    except Exception as e:
        print(e)
        raise e

def extract_anime_producers(anime:dict) -> list:
    try:
        anime_id = anime.get('mal_id', None)
        producers : list[dict]=  anime['producers']
        anime_producers = []
        for producer in producers:
            anime_producers.append((
                anime_id,
                producer.get('mal_id',None),
                producer.get('type',None),
                producer.get('name',None),
                producer.get('url',None),
            ))
        return anime_producers
    except Exception as e:
        print(e)
        raise e

def extract_anime_air(anime:dict) ->tuple:
    try:
        anime_id = anime.get('mal_id', None)
        airing_data = anime.get("aired",{})
        data = (
            anime_id,
            airing_data.get('from',None),
            airing_data.get('to',None),
            airing_data.get('string',None),
        )
        return data
    except Exception as e:
        print(e)
        raise e

