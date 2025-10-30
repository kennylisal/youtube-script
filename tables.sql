-- seasons table
CREATE TABLE IF NOT EXISTS seasons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    season TEXT NOT NULL,
    UNIQUE(year, season)
);

-- anime table
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
    FOREIGN KEY(season_id) REFERENCES seasons(id)
);

-- themes table
CREATE TABLE IF NOT EXISTS themes (
    mal_id INTEGER PRIMARY KEY,
    name TEXT,
    url TEXT,
    count INTEGER
);

-- genres table
CREATE TABLE IF NOT EXISTS genres (
    mal_id INTEGER PRIMARY KEY,
    name TEXT,
    url TEXT,
    count INTEGER
);

-- demographics table
CREATE TABLE IF NOT EXISTS demographics (
    mal_id INTEGER PRIMARY KEY,
    name TEXT,
    url TEXT,
    count INTEGER
);

-- explicit_genres table
CREATE TABLE IF NOT EXISTS explicit_genres (
    mal_id INTEGER PRIMARY KEY,
    name TEXT,
    url TEXT,
    count INTEGER
);

-- anime_demographics table
CREATE TABLE IF NOT EXISTS anime_demographics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER,
    demographic_id INTEGER,
    FOREIGN KEY(anime_id) REFERENCES anime(mal_id),
    FOREIGN KEY(demographic_id) REFERENCES demographics(mal_id),
    UNIQUE(anime_id,demographic_id)
);

-- anime_themes table
CREATE TABLE IF NOT EXISTS anime_themes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER,
    theme_id INTEGER,
    FOREIGN KEY(anime_id) REFERENCES anime(mal_id),
    FOREIGN KEY(theme_id) REFERENCES themes(mal_id),
    UNIQUE(anime_id,theme_id)
);

-- anime_genres table
CREATE TABLE IF NOT EXISTS anime_genres (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER,
    genre_id INTEGER,
    FOREIGN KEY(anime_id) REFERENCES anime(mal_id),
    FOREIGN KEY(genre_id) REFERENCES genres(mal_id),
    UNIQUE(anime_id,genre_id)
);

-- anime_explicit_genres table
CREATE TABLE IF NOT EXISTS anime_explicit_genres (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER,
    explicit_genre_id INTEGER,
    FOREIGN KEY(anime_id) REFERENCES anime(mal_id),
    FOREIGN KEY(explicit_genre_id) REFERENCES explicit_genres(mal_id),
    UNIQUE(anime_id,explicit_genre_id)
);

-- anime_broadcast table
CREATE TABLE IF NOT EXISTS anime_broadcast (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER,
    day TEXT,
    time TEXT,
    timezone TEXT,
    string_text TEXT,
    FOREIGN KEY(anime_id) REFERENCES anime(mal_id)
);

-- anime_studios table
CREATE TABLE IF NOT EXISTS anime_studios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER,
    studio_id INTEGER,
    type TEXT,
    name TEXT,
    url TEXT,
    FOREIGN KEY(anime_id) REFERENCES anime(mal_id)
);

-- anime_producers table
CREATE TABLE IF NOT EXISTS anime_producers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER,
    producer_id INTEGER,
    type TEXT,
    name TEXT,
    url TEXT,
    FOREIGN KEY(anime_id) REFERENCES anime(mal_id)
);

-- anime_aired_schedule table
CREATE TABLE IF NOT EXISTS anime_aired_schedule (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER,
    aired_from DATETIME,
    aired_to DATETIME,
    string_text TEXT,
    FOREIGN KEY(anime_id) REFERENCES anime(mal_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_year_season ON seasons(year, season);
CREATE INDEX IF NOT EXISTS idx_anime_season ON anime(season_id);


--Seksi Pertama ==========================

-- top genre
select count(ag.genre_id) as count, g.name 
from anime_genres ag, genres g 
where ag.genre_id = g.mal_id 
group by genre_id 
order by count desc;

-- most popular word -> cek anime_indexer.py

-- cek genre untuk spesifik tahun
SELECT g.name as 'genre_name', COUNT(ag.genre_id) as total
FROM anime a, anime_aired_schedule ar, anime_genres ag, genres g
WHERE a.mal_id = ar.anime_id
AND strftime('%Y', ar.aired_from) = '2024'
AND ag.anime_id = a.mal_id
AND g.mal_id = ag.genre_id
GROUP BY ag.genre_id
ORDER BY total desc;

-- cek theme untuk spesifik tahun
SELECT t.name as 'theme_name', COUNT(ath.theme_id) as total
FROM anime a, anime_aired_schedule ar, anime_themes ath, themes t
WHERE a.mal_id = ar.anime_id
AND strftime('%Y', ar.aired_from) = '2024'
AND ath.anime_id = a.mal_id
AND t.mal_id = ath.theme_id
GROUP BY ath.theme_id
ORDER BY total desc;

--Seksi Kedua ==========================

--Isekai vs Entire year
SELECT t.name as 'theme_name', COUNT(ath.theme_id) as total
FROM anime a, anime_aired_schedule ar, anime_themes ath, themes t
WHERE a.mal_id = ar.anime_id
AND strftime('%Y', ar.aired_from) = '2024'
AND ath.anime_id = a.mal_id
AND t.mal_id = ath.theme_id
AND t.name = 'Isekai'
GROUP BY ath.theme_id
ORDER BY total desc;

select count(*) as total
FROM anime a, anime_aired_schedule ar
WHERE a.mal_id = ar.anime_id
AND strftime('%Y', ar.aired_from) = '2024';

--jumlah anime tiap tahun
SELECT strftime('%Y', ar.aired_from) AS year, COUNT(DISTINCT a.mal_id) AS total_isekai_anime
FROM anime a
JOIN anime_aired_schedule ar ON a.mal_id = ar.anime_id
JOIN anime_themes ath ON ath.anime_id = a.mal_id
JOIN themes t ON t.mal_id = ath.theme_id
WHERE t.name = 'Isekai'
GROUP BY year
ORDER BY year ASC;


--isekai on each year
SELECT s.year, s.season, COUNT(DISTINCT a.mal_id) AS isekai_count
FROM seasons s, anime a, anime_themes ath, themes t
WHERE a.mal_id = ath.anime_id
AND a.season_id = s.id
AND ath.theme_id = t.mal_id
AND t.name = 'Isekai'
GROUP BY s.year, s.season
ORDER BY s.year, s.season;

--most genre with isekai
SELECT g.name AS genre_name, COUNT(DISTINCT ag.anime_id) AS anime_count
FROM genres g
JOIN anime_genres ag ON g.mal_id = ag.genre_id
JOIN anime_themes ath ON ag.anime_id = ath.anime_id
JOIN themes t ON ath.theme_id = t.mal_id
WHERE t.name = 'Isekai'
GROUP BY g.name
ORDER BY anime_count DESC;

--earliest isekai anime
SELECT a.title, json_extract(a.aired_json, '$.from') AS aired_from
FROM anime a
JOIN anime_themes ath ON a.mal_id = ath.anime_id
JOIN themes t ON ath.theme_id = t.mal_id
WHERE t.name = 'Isekai'
ORDER BY aired_from ASC
LIMIT 10;