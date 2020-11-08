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
TABLES = {}
TABLES['Albums'] = ('create table Albums '
                    '(album_id CHAR(22) PRIMARY KEY,'
                    'name VARCHAR(200), '
                    'total_tracks INT,'
                    'album_type VARCHAR(10),'
                    'release_date DATE);')
db.cnx.commit()

db.create_tables(TABLES)

d = sp.get_new_releases(max_number_of_albums=35, initial_offset=35, filename='../resources/data/trial.csv')
print(d)
db.insert_values_from_dict('Albums', d)
db.cnx.commit()
#
#
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