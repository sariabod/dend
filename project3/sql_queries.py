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
duration NUMERIC,
year INTEGER
);
"""

songplay_table_create = """
CREATE TABLE songplays
(
songplay_id INT IDENTITY(1,1),
row_id VARCHAR,
start_time BIGINT NOT NULL,
user_id INTEGER NOT NULL,
level VARCHAR(20),
song_id VARCHAR,
artist_id VARCHAR,
session_id INTEGER,
location TEXT,
user_agent TEXT,
PRIMARY KEY (songplay_id)
);
"""

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

song_table_create = """
CREATE TABLE songs
(
song_id VARCHAR NOT NULL,
title TEXT,
artist_id VARCHAR NOT NULL,
year INTEGER ,
duration NUMERIC,
PRIMARY KEY (song_id)
)
"""

artist_table_create = """
CREATE TABLE artists
(
artist_id VARCHAR NOT NULL,
name TEXT,
location TEXT,
latitude NUMERIC,
longitude NUMERIC,
PRIMARY KEY (artist_id)
)
"""

time_table_create = """
CREATE TABLE time
(
start_time BIGINT,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER,
PRIMARY KEY (start_time)
)
"""

# STAGING TABLES

staging_events_copy = """
copy staging_events from {}
iam_role {}
FORMAT AS JSON {};
""".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = """
copy staging_songs from {}
iam_role {}
FORMAT AS JSON 'auto';
""".format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = """
create temp table songplay_stage (like songplays);
INSERT INTO songplay_stage (row_id, start_time, user_id, level,
song_id, artist_id, session_id, location, user_agent)
(SELECT (se.ts || '_' || se.userid || '_' || se.sessionid) as row_id,
se.ts, se.userid, se.level, s.song_id, s.artist_id, se.sessionid, se.location, se.useragent
FROM staging_events se
left join songs s
on s.title=se.song
where page='NextSong' order by ts);

update songplays
set song_id = songplay_stage.song_id,
artist_id = songplay_stage.artist_id
from songplay_stage
where songplays.row_id = songplay_stage.row_id;

delete from songplay_stage
using songplays
where songplay_stage.row_id = songplays.row_id;

insert into songplays (row_id, start_time, user_id, level, song_id, artist_id, session_id,
location, user_agent)
(select distinct(row_id), start_time, user_id, level, song_id, artist_id, session_id,
location, user_agent from songplay_stage);

drop table songplay_stage;
"""

user_table_insert = """
create temp table users_stage (like users);
INSERT INTO users_stage (user_id, first_name, last_name, gender, level)
(SELECT distinct(userid), firstname, lastname, gender, level FROM staging_events
where userid is not NULL);

update users
set level = users_stage.level
from users_stage
where users.user_id = users_stage.user_id;

delete from users_stage
using users
where users_stage.user_id = users.user_id;

insert into users
(select distinct(user_id), first_name, last_name, gender, level from users_stage);

drop table users_stage;
"""

song_table_insert = """
create temp table songs_stage (like songs);
INSERT INTO songs_stage (song_id, title, artist_id, year, duration)
(SELECT distinct(song_id), title, artist_id, year, duration
FROM staging_songs where song_id is not NULL);

delete from songs_stage
using songs
where songs_stage.song_id = songs.song_id;

insert into songs
(select distinct(song_id), title, artist_id, year, duration from songs_stage);

drop table songs_stage;
"""

artist_table_insert = """
create temp table artists_stage (like artists);
INSERT INTO artists_stage (artist_id, name, location, longitude, latitude)
(SELECT distinct(artist_id), artist_name, artist_location, artist_longitude,
artist_latitude FROM staging_songs where artist_id is not NULL);

delete from artists_stage
using artists
where artists_stage.artist_id = artists.artist_id;

insert into artists
(select distinct(artist_id), name, location, longitude, latitude from artists_stage);

drop table artists_stage;
"""

time_table_insert = """
create temp table time_stage (like time);
INSERT INTO time_stage (start_time, hour, day, week, month, year, weekday)
(SELECT distinct(ts),
extract(hour from TIMESTAMP 'epoch' + ts * INTERVAL '1 Second ') as hour,
extract(day from TIMESTAMP 'epoch' + ts * INTERVAL '1 Second ') as day,
extract(week from TIMESTAMP 'epoch' + ts * INTERVAL '1 Second ') as week,
extract(month from TIMESTAMP 'epoch' + ts * INTERVAL '1 Second ') as month,
extract(year from TIMESTAMP 'epoch' + ts * INTERVAL '1 Second ') as year,
case extract(dow from TIMESTAMP 'epoch' + ts * INTERVAL '1 Second ')
when 0 or 6 or 7
then 0 else 1
end
FROM staging_events
order by ts);
delete from time_stage
using time
where time_stage.start_time = time.start_time;
insert into time
(select distinct(start_time), hour, day, week, month, year, weekday from time_stage);
drop table time_stage;
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
        songplay_table_create, user_table_create, song_table_create,
        artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
        songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop,
        time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,
        time_table_insert, songplay_table_insert]
