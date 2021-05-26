# DROP TABLES

# FACT TABLE ----
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
# DIMENSION TABLES ----
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Here a type SERIAL is used for songplay_id so that postgresql auto increments this id
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays \
                            (songplay_id SERIAL PRIMARY KEY, start_time bigint NOT NULL, user_id int, level varchar, \
                             song_id varchar, artist_id varchar, session_id int, location varchar, \
                             user_agent varchar); 
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users \
                        (user_id int PRIMARY KEY, first_name varchar, last_name varchar, \
                         gender char, level varchar);
""")

# Since we will use the title and duration to uniquely identify songs, they'll be added to a composite key
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs \
                        (song_id varchar, title varchar , artist_id varchar NOT NULL, \
                        year int, duration decimal, 
                        PRIMARY KEY (song_id, title, duration));
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists \
                          (artist_id varchar PRIMARY KEY, name varchar NOT NULL UNIQUE, location varchar, \
                           latitude decimal, longitude decimal);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time \
                        (start_time bigint PRIMARY KEY, hour int, day int, \
                         week int, month int, year int, weekday varchar);
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays \
                            (start_time, user_id, level, \
                             song_id, artist_id, session_id, location, \
                             user_agent) \
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) \
                        VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (user_id) \
                        DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) \
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (song_id, title, duration) \
                        DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                          VALUES (%s, %s, %s, %s, %s) \
                          ON CONFLICT (artist_id) \
                          DO NOTHING;
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s) \
                        ON CONFLICT (start_time) \
                        DO NOTHING;
""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, songs.artist_id \
                  FROM songs JOIN artists ON songs.artist_id=artists.artist_id \
                  WHERE (%s=songs.title AND %s=artists.name AND %s=songs.duration);
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]