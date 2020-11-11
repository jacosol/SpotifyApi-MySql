# @Created on  : 28/10/2020 01:33
# @Author      : Jacopo Solari
# @Email       : jaco.solari@gmail.com
# @File        : trials.py
import json

from DatabaseManger import DataBaseManager
import numpy as np

from SpotifyAPI import SpotifyAPI

sp = SpotifyAPI(
)

# db = DataBaseManager(logging_level=1)
#
# db.drop_table('Albums')
# TABLES = {}
# TABLES['Albums'] = ('create table Albums '
#                     '(album_id VARCHAR(22) PRIMARY KEY,'
#                     'name VARCHAR(200),'
#                     'total_tracks INT);')

# db.create_tables(TABLES)
# qry = 'INSERT INTO Albums (album_id, name, total_tracks) VALUES (%s, %s, %s);'
# db.cursor.executemany(qry, [('asdfa', 'asdfb', 3), ('ddd', 'sdfa', 4)])
# db.insert_values_from_file('Albums', filepath='../resources/data/trial.csv', primary_key='album_id')
# db.cnx.commit()

db = DataBaseManager(logging_level=1)
#
db.drop_table('Albums')
db.drop_table('Artists')
db.drop_table('Authorship')
db.drop_table('Tracks')
TABLES = {}
TABLES['Albums'] = ('create table Albums '
                    '(album_id CHAR(22) PRIMARY KEY,'
                    'name VARCHAR(200), '
                    'total_tracks INT,'
                    'album_type VARCHAR(10),'
                    'release_date DATE);')

TABLES['Artists'] = ('create table Artists '
                     '(artist_id CHAR(22) PRIMARY KEY,'
                     'name VARCHAR(200), '
                     'type VARCHAR(10),'
                     'popularity INT,'
                     'uri VARCHAR(100),'
                     'followers INT);')
TABLES['Tracks'] = ('create table Tracks '
                    '(track_id CHAR(22) PRIMARY KEY,'
                    'name VARCHAR(200),'
                    'duration_ms INT,'
                    'tempo FLOAT,'
                    'album_id CHAR(22),'
                    'explicit VARCHAR(10),'
                    'song_key INT,'
                    'mode INT,'
                    'time_signature INT,'
                    'acousticness FLOAT,'
                    'danceability FLOAT,'
                    'energy FLOAT,'
                    'instrumentalness FLOAT,'
                    'speechiness FLOAT,'
                    'valence FLOAT,'
                    'track_number INT);')

TABLES['Authorship'] = ('create table Authorship '
                        '(artist_id CHAR(22),'
                        'album_id CHAR(22));')

db.cnx.commit()
db.create_tables(TABLES)

albums, artists, authors, tracks = sp.get_new_releases(max_number_of_albums=35, initial_offset=35,
                                                       filename='../resources/data/trial.csv')
db.insert_values_from_dict('Albums', albums)
db.insert_values_from_dict('Artists', artists)
db.insert_values_from_dict('Authorship', authors)
db.insert_values_from_dict('Tracks', tracks)
db.cnx.commit()

do = 0
if do:
    # db.select('*') \
    #     .fromm('Albums') \
    #     .where(condition='release_date = %s', args=(datetime.date(2020,10,22),)) \
    #     .run_query()

    db.drop_table('Albums')
    TABLES = {}
    TABLES['Albums'] = ('create table Albums '
                        '(album_id CHAR(22) PRIMARY KEY,'
                        'name VARCHAR(200), '
                        'total_tracks INT,'
                        'album_type VARCHAR(10),'
                        'release_date DATE);')
    db.cnx.commit()

    db.create_tables(TABLES)
    db.insert_values_from_file(table='Albums', filepath='../resources/data/trial.csv', primary_key='album_id')
    db.cnx.commit()
