# @Created on  : 27/10/2020 16:46
# @Author      : Jacopo Solari
# @Email       : jaco.solari@gmail.com
# @File        : retrieve_data.py

"""
This script will create the needed object to fill the database with data
"""
from SpotifyAPI import SpotifyAPI

start_year = 1999
end_year = 2020

sp = SpotifyAPI(
)


sp.db.drop_table('Albums')
sp.db.drop_table('Artists')
sp.db.drop_table('Authorship')
sp.db.drop_table('Tracks')
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

sp.db.cnx.commit()
sp.db.create_tables(TABLES)

albums, artists, authors, tracks = sp.get_data_over_time_period(start_year=start_year, end_year=end_year,
                                                                max_number_of_albums=20,#int(1e10),
                                                                initial_offset=0, add_inline=True)
sp.db.cnx.commit()




