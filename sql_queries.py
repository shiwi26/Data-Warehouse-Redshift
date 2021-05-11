import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplay;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists song;"
artist_table_drop = "drop table if exists artist;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create= ("""
create table staging_events(
artist text,
auth text,
firstName text,
gender text,
itemInSession integer,
lastName text,
length float,
level text,
location text,
method text,
page text,
registration float,
sessionId integer,
song text,
status integer,
ts timestamp,
userAgent text,
userId integer
)
""")

staging_songs_table_create = ("""
create table staging_songs(
num_songs integer,
artist_id text,
artist_latitude float,
artist_longitude float,
artist_location text,
artist_name text,
song_id text,
title text,
duration float,
year integer
)
""")

songplay_table_create = ("""
create table songplay(
songplay_id INTEGER IDENTITY(0,1) primary key, 
start_time timestamp sortkey, 
user_id integer, 
level text, 
song_id text, 
artist_id text, 
session_id integer, 
location text, 
user_agent text
)
""")

user_table_create = ("""
create table users(
user_id INTEGER primary key sortkey, 
first_name text, 
last_name text, 
gender text, 
level text
)
""")

song_table_create = ("""
create table song(
song_id text primary key sortkey, 
title text, 
artist_id text, 
year integer, 
duration float
)
""")

artist_table_create = ("""
create table artist(
artist_id text primary key sortkey, 
name text, 
location text, 
latitude float, 
longitude float
)
""")

time_table_create = ("""
create table time(
start_time timestamp primary key sortkey, 
hour integer, 
day integer, 
week integer, 
month integer, 
year integer, 
weekday text
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as JSON {}
timeformat as 'epochmillisecs';
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as JSON 'auto';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select distinct(e.ts) as start_time,
e.userId as user_id,
e.level as level,
s.song_id as song_id,
s.artist_id as artist_id,
e.sessionId as session_id,
e.location as location,
e.userAgent as user_agent
from staging_events e join staging_songs s on s.title = e.song and s.artist_name = e.artist
where e.page = 'NextSong'
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
select distinct(userId) as user_id,
firstName as first_name,
lastName as last_name,
gender,
level
from staging_events
where userId is not null
""")

song_table_insert = ("""
insert into song (song_id, title, artist_id, year, duration)
select distinct(song_id) as song_id,
title,
artist_id,
year,
duration
from staging_songs
where song_id is not null
""")

artist_table_insert = ("""
insert into artist (artist_id, name, location, latitude, longitude)
select distinct(artist_id) as artist_id,
artist_name as name,
artist_location as location,
artist_latitude as latitude,
artist_longitude as longitude
from staging_songs
where artist_id is not null
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select distinct(ts) as start_time,
extract(hour from ts) as hour,
extract(day from ts) as day,
extract(week from ts) as week,
extract(month from ts) as month,
extract(year from ts) as year,
extract(dayofweek from ts) as weekday
from staging_events
where ts is not null
""")

# Testing
staging_events_rows = ("""
select count(1) from staging_events;
""")

staging_songs_rows = ("""
select count(1) from staging_songs;
""")

songplay_rows = ("""
select count(1) from songplay;
""")

users_rows = ("""
select count(1) from users;
""")

songs_rows = ("""
select count(1) from song;
""")

artists_rows = ("""
select count(1) from artist;
""")

time_rows = ("""
select count(1) from time;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
testing_queries = [staging_events_rows, staging_songs_rows, songplay_rows, users_rows, songs_rows, artists_rows, time_rows]