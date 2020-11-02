# @Created on  : 28/10/2020 01:33
# @Author      : Jacopo Solari
# @Email       : jaco.solari@gmail.com
# @File        : trials.py
from DatabaseManger import DataBaseManager
import numpy as np

db = DataBaseManager(logging_level=1)

db.drop_table('Albums')
TABLES = {}
TABLES['Albums'] = ('create table Albums '
                    '(album_id VARCHAR(22) PRIMARY KEY,'
                    'name VARCHAR(200),'
                    'total_tracks INT);')

db.create_tables(TABLES)
# qry = 'INSERT INTO Albums (album_id, name, total_tracks) VALUES (%s, %s, %s);'
# db.cursor.executemany(qry, [('asdfa', 'asdfb', 3), ('ddd', 'sdfa', 4)])
db.insert_values_from_file('Albums', filepath='../resources/data/trial.csv', primary_key='album_id')
db.cnx.commit()

# db = DataBaseManager(logging_level=1)
#
# db.drop_table('Albums')
# TABLES = {}
# TABLES['Albums'] = ('create table Albums '
#                     '(album_id CHAR(22) PRIMARY KEY,'
#                     'name VARCHAR(200), '
#                     'total_tracks INT,'
#                     'album_type VARCHAR(10),'
#                     'release_date DATE);')
# db.cnx.commit()
#
# db.create_tables(TABLES)
# # db.insert_values_from_dict('Albums', {'name': ['a', 'b'], 'total_tracks': [1,2]})
#
#
# do = 1
# if do:
#
#     db.select('*') \
#         .fromm('Albums') \
#         .where(condition='release_date = %s', args=(datetime.date(2020,10,22),)) \
#         .run_query()
#
#     db.drop_table('Albums')
#     TABLES = {}
#     TABLES['Albums'] = ('create table Albums '
#                         '(album_id CHAR(22) PRIMARY KEY,'
#                         'name VARCHAR(200), '
#                         'total_tracks INT,'
#                         'album_type VARCHAR(10),'
#                         'release_date DATE);')
#     db.cnx.commit()
#
#     db.create_tables(TABLES)
#     db.insert_values_from_file(table='Albums', filepath='../../resources/data/trial.csv', primary_key='album_id')
#     db.cnx.commit()