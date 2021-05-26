# Sparkify Database

## Motivation
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal is to create a Postgres database with tables designed to optimize queries on song play analysis, and also an ETL pipeline for this analysis.

## Implementation
A Postgres database will be created using a star schema with fact and dimension tables. Python will be used to write the ETL pipeline, which will access the json files (for the songs and for the user acivity) from local directories and insert this data into the relevant datagbase tables.


### Database Design
As mentioned, a star schema will be employed here. 

##### Fact table
* **songplays** will serve as the fact table and will (among other attributes) contain foreign keys for the following dimension tables.

##### Dimension table
* **users** contains information about Sparkify users and has the following fields: *user_id, first_name, last_name, gender, level* (paid for free)
* **songs** contains information about Sparkify songs and has these fields: *song_id, title, artist_id year, duration* (length of song)
* **artists** contains information about music artists and has these fields: artist_id, name, location, latitude, longitude
* **time** contains the components of the timestamps of the log data, having these fields: *start_time, hour, day, week, month, year, weekday*


This schema is used to facilitate business matric analysis strictly through the fact table. If it's desired to know how many unique songs are played per hour, how many unique artists are played in a given time window, etc., this can all be carried out by queries on the **songplays** fact table. For further details about *which* unique artist is played the most in a given time window, or *when* a specific song title is played most, then the dimension tables must be accessed. 

#### Database creation
The database is created by running the *create_tables.py* file. This file does the following:
1. Creates the sparkify database if it does not yet exist
2. Drops all tables that might already exist
3. Creates all required tables (the ones listed above) 

The SQL queries used in *create_tables.py* can be found in the *sql_queries.py* file.  
  
  
  
### ETL pipeline
The ETL pipeline is contained in the *etl.py* file. It contains the following steps:

##### 1. Process song data
Each of the JSON files stored in *data/song_data* is read and the content is used to upsert the **songs** and **artists** tables. 

Some special notes on the `INSERT` query handling. 
* Since an artist can have multiple songs, the `INSERT` query includes conflict handling for the *artist_id*, in which case the *artist_id* is not updated but the other fields may be updated. 
* Since songs will need to be uniquely identified by the combination of their titles and durations (since something like a *song_id* is not available in the log data), the rows in the **songs** table will have a composite `PRIMARY KEY` containing *song_id*, *title*, and *duration*.

##### 2. Process log data
Likewise, each of the JSON log files stored in *data/log_data* is read and the content is used to upsert the **time**, **users**, and finally the **songplays** tables. 

In order to generate data for the **songplays** table, the *song_id* and *artist_id* values are needed. However, these values are not explicitly included in the JSON log files. In order to determine the *song_id* and *artist_id* for a specific song, a `JOIN` is used on the **songs** and **artists** tables, matching on the *title* of the song, the *duration* of the song, and the *name* of the artist. 


## How to Run
1. Run *create_tables.py* to create the Sparkify database and its tables.
2. Run *etl.py* to run the ETL pipeline.


## Example Analysis
Here are some example queries that can be run on the database. 

    SELECT SUM(songplays.songplay_id) FROM songplays
   
*Output*:  
TOTAL STREAMS  
23259610  


    SELECT users.first_name || ' ' || users.last_name full_name, SUM(songplays.user_id) \
                         FROM (songplays JOIN users ON songplays.user_id=users.user_id) \
                         GROUP BY full_name \
                         ORDER BY SUM(songplays.user_id) DESC LIMIT 5

*Output*:  
TOP 5 USERS WITH MOST LISTENS  
('Kate Harrell', 54029)  
('Tegan Levine', 53200)  
('Chloe Cuevas', 33761)  
('Mohammad Rodriguez', 23760)  
('Jacob Klein', 21097)    


    SELECT songplays.level, SUM(songplays.songplay_id) \
                           FROM songplays \
                           GROUP BY songplays.level
*Output*:  
STREAMING COUNTS BY ACCOUNT LEVEL  
('free', 3736127)  
('paid', 19523483) 


    SELECT time.day, SUM(songplays.songplay_id) \
                                FROM (songplays JOIN time ON songplays.start_time=time.start_time) \
                                GROUP BY time.day \
                                ORDER BY SUM(songplays.songplay_id) DESC LIMIT 5

*Output*:  
TOP 5 STREAMING DAYS OF THE MONTH  
(21, 2338824)  
(28, 2143878)  
(24, 2092339)  
(16, 2024693)  
(15, 1798166)  