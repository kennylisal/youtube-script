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
    members INTEGER,
    favorites INTEGER,
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

--average isekai
SELECT AVG(a.score) AS average_isekai_score
FROM anime a
JOIN anime_themes ath ON a.mal_id = ath.anime_id
JOIN themes t ON ath.theme_id = t.mal_id
WHERE t.name = 'Isekai';

--above average isekai, based on isekai average
SELECT a.title, a.score
FROM anime a
JOIN anime_themes ath ON a.mal_id = ath.anime_id
JOIN themes t ON ath.theme_id = t.mal_id
WHERE t.name = 'Isekai'
AND a.score > (
    SELECT AVG(a2.score)
    FROM anime a2
    JOIN anime_themes ath2 ON a2.mal_id = ath2.anime_id
    JOIN themes t2 ON ath2.theme_id = t2.mal_id
    WHERE t2.name = 'Isekai'
)
ORDER BY a.score DESC
LIMIT 20;

--overall average anime score
SELECT AVG(a.score) AS overall_average_score
FROM anime a;
--above average isekai, based on overall anime 
SELECT a.title, a.score
FROM anime a
JOIN anime_themes ath ON a.mal_id = ath.anime_id
JOIN themes t ON ath.theme_id = t.mal_id
WHERE t.name = 'Isekai'
AND a.score > (
    SELECT AVG(a2.score)
    FROM anime a2
)
ORDER BY a.score DESC
LIMIT 20;

--top isekai 
SELECT a.title, a.score
FROM anime a
JOIN anime_themes ath ON a.mal_id = ath.anime_id
JOIN themes t ON ath.theme_id = t.mal_id
WHERE t.name = 'Isekai'
ORDER BY a.score DESC
LIMIT 20;

--top studio that made isekai
SELECT 
    anime_studios.name AS studio_name, 
    COUNT(DISTINCT anime_studios.anime_id) AS isekai_count
FROM 
    anime_studios
JOIN 
    anime ON anime_studios.anime_id = anime.mal_id
JOIN 
    anime_themes ON anime.mal_id = anime_themes.anime_id
JOIN 
    themes ON anime_themes.theme_id = themes.mal_id
WHERE 
    themes.name = 'Isekai'
GROUP BY 
    anime_studios.studio_id, anime_studios.name
ORDER BY 
    isekai_count DESC;

--anime that made by studio X
SELECT DISTINCT a.title, a.title_english, a.year, a.score
FROM anime a
JOIN anime_studios ast ON a.mal_id = ast.anime_id
JOIN anime_themes ath ON a.mal_id = ath.anime_id
JOIN themes t ON ath.theme_id = t.mal_id
WHERE ast.name = 'J.C.Staff'
AND t.name = 'Isekai'
ORDER BY a.year DESC, a.score DESC;

--each year anime placement based on member
WITH ranked_anime AS (
  SELECT 
    a.mal_id,
    a.title,
    a.title_english,
    a.year,
    a.members,
    strftime('%Y', s.aired_from) AS schedule_year,
    ROW_NUMBER() OVER (
      PARTITION BY strftime('%Y', s.aired_from) 
      ORDER BY a.members DESC
    ) AS rn
  FROM anime a
  JOIN anime_aired_schedule s ON a.mal_id = s.anime_id
  WHERE s.aired_from IS NOT NULL
)
SELECT 
  mal_id,
  title,
  title_english,
  year,
  members,
  schedule_year
FROM ranked_anime
WHERE rn = 1
ORDER BY schedule_year;

--each year anime placement based on score
WITH ranked_anime AS (
  SELECT 
    a.mal_id,
    a.title,
    a.title_english,
    a.year,
    a.score,
    strftime('%Y', s.aired_from) AS schedule_year,
    ROW_NUMBER() OVER (
      PARTITION BY strftime('%Y', s.aired_from) 
      ORDER BY a.score DESC
    ) AS rn
  FROM anime a
  JOIN anime_aired_schedule s ON a.mal_id = s.anime_id
  WHERE s.aired_from IS NOT NULL
)
SELECT 
  mal_id,
  title,
  title_english,
  year,
  score,
  schedule_year
FROM ranked_anime
WHERE rn = 1
ORDER BY schedule_year;

--source terbanyak isekai
SELECT 
    anime.source AS source_name, 
    COUNT(DISTINCT anime.mal_id) AS isekai_count
FROM 
    anime
JOIN 
    anime_themes ON anime.mal_id = anime_themes.anime_id
JOIN 
    themes ON anime_themes.theme_id = themes.mal_id
WHERE 
    themes.name = 'Isekai'
    AND anime.source IS NOT NULL  -- Optional: Exclude NULL sources if any
GROUP BY 
    anime.source
ORDER BY 
    isekai_count DESC;

-- top genre each year
WITH yearly_genre_counts AS (
  SELECT 
    strftime('%Y', aired_from) AS year,
    themes.name AS genre_name,
    COUNT(DISTINCT anime.mal_id) AS anime_count,
    ROW_NUMBER() OVER (
      PARTITION BY strftime('%Y', aired_from) 
      ORDER BY COUNT(DISTINCT anime.mal_id) DESC
    ) AS rank
  FROM anime_aired_schedule
  JOIN anime ON anime_aired_schedule.anime_id = anime.mal_id
  JOIN anime_themes ON anime.mal_id = anime_themes.anime_id
  JOIN themes ON anime_themes.theme_id = themes.mal_id
  WHERE aired_from IS NOT NULL
  GROUP BY year, genre_name
)
SELECT 
  year,
  genre_name,
  anime_count
FROM yearly_genre_counts
WHERE rank <= 5
ORDER BY year ASC, anime_count DESC;

SELECT 
  strftime('%Y', aired_from) AS year,
  COUNT(DISTINCT anime_id) AS anime_count
FROM anime_aired_schedule
WHERE aired_from IS NOT NULL
GROUP BY year
ORDER BY year ASC;


--average isekai per year
WITH isekai_anime AS (
  SELECT 
    anime.mal_id,
    anime.score,
    strftime('%Y', anime_aired_schedule.aired_from) AS year
  FROM anime
  JOIN anime_themes ON anime.mal_id = anime_themes.anime_id
  JOIN themes ON anime_themes.theme_id = themes.mal_id
  JOIN anime_aired_schedule ON anime.mal_id = anime_aired_schedule.anime_id
  WHERE themes.name = 'Isekai'
    AND anime.score IS NOT NULL
    AND anime_aired_schedule.aired_from IS NOT NULL
)
SELECT 
  year,
  ROUND(AVG(score), 2) AS avg_score,
  COUNT(mal_id) AS anime_count
FROM isekai_anime
GROUP BY year
ORDER BY year ASC;

WITH isekai_ranked AS (
  SELECT 
    anime.mal_id,
    anime.title,
    anime.score,
    strftime('%Y', anime_aired_schedule.aired_from) AS year,
    ROW_NUMBER() OVER (
      PARTITION BY strftime('%Y', anime_aired_schedule.aired_from)
      ORDER BY anime.score DESC
    ) AS rank
  FROM anime
  JOIN anime_themes ON anime.mal_id = anime_themes.anime_id
  JOIN themes ON anime_themes.theme_id = themes.mal_id
  JOIN anime_aired_schedule ON anime.mal_id = anime_aired_schedule.anime_id
  WHERE themes.name = 'Isekai'
    AND anime.score IS NOT NULL
    AND anime_aired_schedule.aired_from IS NOT NULL
)
SELECT 
  year,
  title,
  score
FROM isekai_ranked
WHERE rank = 1
ORDER BY year ASC;

--isekai aired on year X
SELECT 
  anime.mal_id,
  anime.title,
  anime.title_english,
  anime.score,
  anime_aired_schedule.aired_from,
  anime_aired_schedule.aired_to
FROM anime
JOIN anime_themes ON anime.mal_id = anime_themes.anime_id
JOIN themes ON anime_themes.theme_id = themes.mal_id
JOIN anime_aired_schedule ON anime.mal_id = anime_aired_schedule.anime_id
WHERE themes.name = 'Isekai'
  AND strftime('%Y', anime_aired_schedule.aired_from) = '2025'
  AND anime_aired_schedule.aired_from IS NOT NULL
ORDER BY anime.score DESC NULLS LAST, anime_aired_schedule.aired_from ASC;

--golden era
--how many genre came out each year
WITH theme_anime_counts AS (
  SELECT 
    strftime('%Y', anime_aired_schedule.aired_from) AS year,
    themes.name AS theme_name,
    COUNT(DISTINCT anime.mal_id) AS anime_count
  FROM anime_aired_schedule
  JOIN anime ON anime_aired_schedule.anime_id = anime.mal_id
  JOIN anime_themes ON anime.mal_id = anime_themes.anime_id
  JOIN themes ON anime_themes.theme_id = themes.mal_id
  WHERE anime_aired_schedule.aired_from IS NOT NULL
    AND themes.name = 'Isekai' 
  GROUP BY year, theme_name
)
SELECT 
  year,
  theme_name,
  anime_count
FROM theme_anime_counts
ORDER BY year ASC, anime_count DESC;

WITH explicit_genre_anime_counts AS (
  SELECT 
    strftime('%Y', anime_aired_schedule.aired_from) AS year,
    themes.name AS theme_name,
    COUNT(DISTINCT anime.mal_id) AS anime_count
  FROM anime_aired_schedule
  JOIN anime ON anime_aired_schedule.anime_id = anime.mal_id
  JOIN anime_themes ON anime.mal_id = anime_themes.anime_id
  JOIN themes ON anime_themes.theme_id = themes.mal_id
  WHERE anime_aired_schedule.aired_from IS NOT NULL
    AND themes.name = 'Isekai' 
  GROUP BY year, theme_name
)
SELECT 
  year,
  theme_name,
  anime_count
FROM explicit_genre_anime_counts
ORDER BY year ASC, anime_count DESC;

WITH explicit_genre_anime_counts AS (
  SELECT 
    strftime('%Y', anime_aired_schedule.aired_from) AS year,
    explicit_genres.name AS explicit_genre_name,
    COUNT(DISTINCT anime.mal_id) AS anime_count
  FROM anime_aired_schedule
  JOIN anime ON anime_aired_schedule.anime_id = anime.mal_id
  JOIN anime_explicit_genres ON anime.mal_id = anime_explicit_genres.anime_id
  JOIN explicit_genres ON anime_explicit_genres.explicit_genre_id = explicit_genres.mal_id
  WHERE anime_aired_schedule.aired_from IS NOT NULL
    AND explicit_genres.name = 'Ecchi' 
  GROUP BY year, explicit_genre_name
)
SELECT 
  year,
  explicit_genre_name,
  anime_count
FROM explicit_genre_anime_counts
ORDER BY year ASC, anime_count DESC;

--day's average score for all recorder years
SELECT 
    ab.day,
    strftime('%Y', aas.aired_from) AS aired_year,
    AVG(a.score) AS avg_score,
    COUNT(a.mal_id) AS anime_count  -- Optional: Shows sample size per group
FROM 
    anime_broadcast ab
JOIN 
    anime a ON ab.anime_id = a.mal_id
JOIN 
    anime_aired_schedule aas ON ab.anime_id = aas.anime_id
WHERE 
    a.score IS NOT NULL  -- Exclude unscored anime
    AND aas.aired_from IS NOT NULL  -- Ensure valid start date for year extraction
GROUP BY 
    ab.day, aired_year
HAVING 
    anime_count >= 5  -- Optional: Filter out groups with too few anime to avoid unreliable averages (adjust threshold as needed)
ORDER BY 
    aired_year ASC, avg_score DESC;

-- overall ammount of isekai came out on spesific day
SELECT 
    ab.day,
    COUNT(DISTINCT a.mal_id) AS isekai_anime_count
FROM 
    anime_themes at
JOIN 
    anime a ON at.anime_id = a.mal_id
JOIN 
    anime_broadcast ab ON at.anime_id = ab.anime_id
WHERE 
    at.theme_id = (SELECT mal_id FROM themes WHERE name = 'Isekai')
GROUP BY 
    ab.day
ORDER BY 
    ab.day ASC;

-- average score of isekai anime on each day -- overall
SELECT 
    ab.day,
    AVG(a.score) AS avg_score,
    COUNT(DISTINCT a.mal_id) AS isekai_anime_count
FROM 
    anime_themes at
JOIN 
    anime a ON at.anime_id = a.mal_id
JOIN 
    anime_broadcast ab ON at.anime_id = ab.anime_id
WHERE 
    at.theme_id = (SELECT mal_id FROM themes WHERE name = 'Isekai')
    AND a.score IS NOT NULL
GROUP BY 
    ab.day
ORDER BY 
    ab.day ASC;

-- show top isekai anime based on broadcast day -overall
WITH ranked_isekai AS (
    SELECT 
        a.title,
        a.score,
        a.year,
        ab.day,
        ROW_NUMBER() OVER (PARTITION BY ab.day ORDER BY a.score DESC) AS rn
    FROM 
        anime_themes at
    JOIN 
        anime a ON at.anime_id = a.mal_id
    JOIN 
        anime_broadcast ab ON at.anime_id = ab.anime_id
    WHERE 
        at.theme_id = (SELECT mal_id FROM themes WHERE name = 'Isekai')
        AND a.score IS NOT NULL
)
SELECT 
    title,
    score,
    year,
    day
FROM 
    ranked_isekai
WHERE 
    rn <= 10
ORDER BY 
    day ASC, score DESC;

-- top isekai anime based on broadcast day
SELECT DISTINCT
    a.title,
    a.score,
    a.year,
    ab.day
FROM 
    anime_broadcast ab
JOIN 
    anime a ON ab.anime_id = a.mal_id
JOIN 
    anime_themes at ON at.anime_id = a.mal_id
WHERE 
    ab.day = 'Mondays'
    AND a.score IS NOT NULL  -- Exclude unscored anime
    AND at.theme_id = (SELECT mal_id FROM themes WHERE name = 'Isekai')
ORDER BY 
    a.year DESC
LIMIT 10;

SELECT DISTINCT day
FROM anime_broadcast
WHERE anime_id = 45576;

-- average score of overall anime on each day
SELECT 
    ab.day,
    AVG(a.score) AS avg_score,
    COUNT(DISTINCT a.mal_id) AS anime_count
FROM 
    anime_broadcast ab
JOIN 
    anime a ON ab.anime_id = a.mal_id
WHERE 
    a.score IS NOT NULL
GROUP BY 
    ab.day
ORDER BY 
    ab.day ASC;

--find top anime on each day
SELECT DISTINCT
    a.title,
    a.score,
    a.year,
    ab.day
FROM 
    anime_broadcast ab
JOIN 
    anime a ON ab.anime_id = a.mal_id
WHERE 
    ab.day = 'Fridays'
    AND a.score IS NOT NULL  -- Exclude unscored anime
ORDER BY 
    a.score DESC
LIMIT 10;