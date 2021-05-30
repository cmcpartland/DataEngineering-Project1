import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

import credentials

def process_song_file(cur, filepath):
    """This function takes in a file path for a JSON file containing song and artist metadata.
    The JSON file is read into a pandas DF and the relevant fields are extracted.
    An entry for the songs table and for the artist table is created out of this data
    and then inserted into the appropriate tables.

    INPUTS:
    * cur is the cursor variable referring to the database connection
    * filepath is the file path to the JSON song file
    """
    # open song file
    df = pd.read_json(filepath, typ='series', dtype=False)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', \
                           'artist_latitude', 'artist_longitude']])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """This function takes in a file path for a JSON file containing log data for user activity.
    The JSON file is read into a pandas DF and the relevant fields are extracted.
    Entries for the time, user, and songplay tables are are created out of this data
    and then inserted into the appropriate tables.

    INPUTS:
    * cur is the cursor variable referring to the database connection
    * filepath is the file path to the JSON log file
    """
    # open log file
    df = pd.read_json(filepath, dtype=False, lines=True)

    # filter by NextSong action
    df = df.loc[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (df['ts'], t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.dayofweek)
    column_labels = ("timestamp", "hour", "day", "week of year", "month", "year", "weekday")
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row['ts'], row['userId'], row['level'], songid, artistid, \
                     row['sessionId'], row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """This function goes through all files in a directory and carries out some other function on each
    JSON file found inside the directory. The file path to the directory should contain only a single
    JSON file type, i.e. log data OR song data, not both.

    INPUTS;
    * cur the cursor to the database connection
    * conn the connection to the database
    * filepath the file path to the directory containing the jSON files
    * func the function that should be applied to each JSON file
    """
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
    """The main function, which is carried out if this file is executed.
    A database connection is created, and then the song data files and log data files are processed.
    """
    # conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=%s password=%s" \
                            % (credentials.USER, credentials.PASSWORD))
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()