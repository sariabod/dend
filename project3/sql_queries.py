import configparser
import sys


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_events_table_create= """
CREATE TABLE staging_events
(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length NUMERIC,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
sessionId INTEGER,
song VARCHAR,
status INTEGER,
ts BIGINT,
userAgent VARCHAR,
userId INTEGER
);
"""

staging_songs_table_create = """
CREATE TABLE staging_songs
(
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude NUMERIC,
artist_longitude NUMERIC,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration NUMERIC
);
"""

songplay_table_create = """
CREATE TABLE songplays
(
songplay_id INT IDENTITY(0, 1),
start_time TIMESTAMP NOT NULL,
user_id INTEGER NOT NULL,
level VARCHAR(20),
song_id INTEGER,
artist_id INTEGER,
session_id INTEGER,
location TEXT,
user_agent TEXT
);
"""

user_table_create = """
CREATE TABLE users
(
user_id INTEGER NOT NULL,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR(1),
level VARCHAR(50)
);
"""

song_table_create = """
CREATE TABLE songs
(
song_id VARCHAR NOT NULL,
title TEXT,
artist_id VARCHAR,
year INTEGER ,
duration NUMERIC
)
"""


artist_table_create = """
CREATE TABLE artists
(
artist_id VARCHAR NOT NULL,
name TEXT,
location TEXT,
latitude NUMERIC,
longitude NUMERIC
)
"""

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

# STAGING TABLES

staging_events_copy = """
copy staging_events from {}
iam_role {}
format as json 'auto';
""".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])


staging_songs_copy = """
copy staging_songs from {}
iam_role {}
format as json 'auto';
""".format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
