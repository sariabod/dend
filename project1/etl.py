import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json
import sys
import warnings

if not sys.warnoptions:
    warnings.simplefilter("ignore")


def process_song_file(cur, filepath):
    # open song file
    with open(filepath) as json_file:  
        song_json = json.load(json_file)

    # insert song record
    song_data = [song_json['song_id'],song_json['title'], song_json['artist_id'], song_json['year'], song_json['duration']]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [song_json['artist_id'],song_json['artist_name'], song_json['artist_location'], song_json['artist_latitude'], song_json['artist_longitude']]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    log_df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = log_df[log_df['page']=='NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    df['hour'] = df['ts'].dt.hour
    df['day'] = df['ts'].dt.day
    df['week'] = df['ts'].dt.week
    df['month'] = df['ts'].dt.month
    df['year'] = df['ts'].dt.year
    df['weekday'] = df['ts'].dt.weekday
    
    column_labels = ['ts','hour','day','week','month','year','weekday']
    time_df = df[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row['song'], row['length'], row['artist']))
        songid, artistid = results if results else None, None

        # insert songplay record
        songplay_data = [row['ts'],row['userId'],row['level'],songid,artistid,row['sessionId'],row['location'],row['userAgent']]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
