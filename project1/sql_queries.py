"""
Fact Table
----------

songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


Dimension Tables
----------------

users - users in the app
user_id, first_name, last_name, gender, level

songs - songs in music database
song_id, title, artist_id, year, duration

artists - artists in music database
artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

"""


# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = """
CREATE TABLE songplays 
(
songplay_id SERIAL PRIMARY KEY,
start_time TIMESTAMP,
user_id INTEGER,
level VARCHAR(20),
song_id INTEGER,
artist_id INTEGER,
session_id INTEGER,
location TEXT,
user_agent TEXT
);
"""

#user_id, first_name, last_name, gender, level
user_table_create = """
CREATE TABLE users
(
user_id INTEGER NOT NULL,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR(1),
level VARCHAR(50),
PRIMARY KEY (user_id)
);
"""

#song_id, title, artist_id, year, duration
song_table_create = """
CREATE TABLE songs
(
song_id VARCHAR NOT NULL,
title TEXT,
artist_id VARCHAR,
year INTEGER ,
duration NUMERIC,
PRIMARY KEY (song_id)
)
"""

#artist_id, name, location, lattitude, longitude
artist_table_create = """
CREATE TABLE artists
(
artist_id VARCHAR NOT NULL,
name TEXT,
location TEXT,
latitude VARCHAR,
longitude VARCHAR,
PRIMARY KEY (artist_id)
)
"""


#start_time, hour, day, week, month, year, weekday
time_table_create = """
CREATE TABLE time
(
start_time TIMESTAMP,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER
)
"""

# INSERT RECORDS
songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE set (first_name, last_name, gender, level) = (EXCLUDED.first_name, EXCLUDED.last_name, EXCLUDED.gender, EXCLUDED.level)
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s,%s,%s,%s,%s)
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (artist_id) DO UPDATE set
(name, location, latitude, longitude) = (EXCLUDED.name, EXCLUDED.location, EXCLUDED.latitude, EXCLUDED.longitude)
"""


time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s,%s,%s,%s,%s,%s,%s)
"""

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id
FROM songs s
LEFT JOIN artists a
ON a.artist_id = s.artist_id
WHERE
s.title = %s and
s.duration = %s and
a.name = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
