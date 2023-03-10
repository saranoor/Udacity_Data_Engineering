import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS STAGING_EVENTS"
staging_songs_table_drop = "DROP TABLE IF EXISTS STAGING_SONGS"
songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS TIME"

# CREATE TABLES
staging_events_table_create = ("""CREATE TABLE staging_events (
    artist VARCHAR, 
    auth VARCHAR, 
    firstName VARCHAR, 
    gender VARCHAR, 
    itemInSession int, 
    lastName VARCHAR,
    length float, 
    level VARCHAR, 
    location VARCHAR,
    method VARCHAR,
    page VARCHAR, 
    registration VARCHAR, 
    sessionId VARCHAR, 
    song VARCHAR, 
    status VARCHAR, 
    ts bigint, 
    userAgent VARCHAR, 
    userId VARCHAR);
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs  (num_songs int, 
    artist_id VARCHAR,
    artist_latitude float, 
    artist_longitude float, 
    artist_location VARCHAR, 
    artist_name VARCHAR, 
    song_id VARCHAR, 
    title VARCHAR, 
    duration float, 
    year int);
""")

songplay_table_create = ("""CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
    start_time TIMESTAMP, 
    user_id VARCHAR, 
    level VARCHAR, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id VARCHAR NOT NULL, 
    location VARCHAR, 
    user_agent VARCHAR,
    FOREIGN KEY (start_time) REFERENCES time(start_time), 
    FOREIGN KEY (user_id) REFERENCES USERS(user_id),
    FOREIGN KEY(song_id) REFERENCES SONGS(song_id), 
    FOREIGN KEY(artist_id) REFERENCES ARTISTS(artist_id));
""")

user_table_create = ("""CREATE TABLE users (user_id VARCHAR PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR);
""")

song_table_create = ("""CREATE TABLE songs (song_id VARCHAR PRIMARY KEY, title VARCHAR, artist_id VARCHAR, year int, duration float);
""")

artist_table_create = ("""CREATE TABLE artists (artist_id VARCHAR PRIMARY KEY, name VARCHAR, location VARCHAR, latitutde float, longitude VARCHAR);
""")

time_table_create = ("""CREATE TABLE time (start_time TIMESTAMP PRIMARY KEY, hour int, day int, week int, month int, year int, weekday VARCHAR)
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
IAM_ROLE {}
JSON {}
region 'us-west-2'
dateformat 'auto'
timeformat as 'auto';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM {}
IAM_ROLE {}
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time, userId as user_id, level, staging_songs.song_id as song_id, staging_songs.artist_id as artist_id, sessionID as session_id, location, userAgent as user_agent 
FROM staging_events LEFT JOIN staging_songs 
ON staging_events.artist=staging_songs.artist_name AND staging_events.song=staging_songs.title AND staging_events.length=staging_songs.duration 
WHERE staging_events.artist IS NOT NULL  AND staging_events.song IS NOT NULL;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events 
where staging_events.firstname is not null AND staging_events.lastname  is not null;;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) SELECT DISTINCT song_id, title, artist_id, year, duration from staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitutde, longitude) SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) select distinct start_time as st, date_part('hour', st), date_part('day', st), date_part('week', st), date_part('month', st), date_part('year', st), date_part('weekday', st) from songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
