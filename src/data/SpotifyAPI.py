# @Created on  : 28/10/2020 01:20
# @Author      : Jacopo Solari
# @Email       : jaco.solari@gmail.com
# @File        : SpotifyAPI.py

import os
import spotipy
import json
import requests
from spotipy.oauth2 import SpotifyClientCredentials
import mysql
import time

from DatabaseManger import DataBaseManager


class SpotifyAPI(object):
    """This class can download data from Spotify API.
    """
    def __init__(self):
        self.authenticate()
        self.db = DataBaseManager(logging_level=-1)

    def authenticate(self):
        my_id = os.environ.get('CLIENT_ID')
        secret_key = os.environ.get('SECRET_KEY')
        client_credentials_manager = SpotifyClientCredentials(client_id=my_id, client_secret=secret_key)
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    def list_all_genres(self):
        pass

    def get_new_releases(self, max_number_of_albums=20000, limit=10, initial_offset=0, filename='../../resources/data/trial.csv'):
        """
        This method downloads all the information about new releases.
        It stores the informations in 4 different dictionaries: albums, artists, tracks and authorship.
        These dict can easily be added to the spotify MySQL database through a @DatabaseManager instance.
        :param max_number_of_albums:
        :param limit:
        :param initial_offset:
        :param filename:
        :return:
        """
        response_albums = {'name':[], 'artist':[], 'artist_spotify_link':[], 'artist_spotify_id':[],
                    'release_date':[], 'album_id':[], 'album_type':[], 'total_tracks':[]}
        response_artists = {'name': [], 'artist_id': [], 'type':[], 'followers':[], 'popularity':[], 'uri':[]}
        response_authorship = {'artist_id': [], 'album_id':[]}
        response_tracks = {'name':[], 'duration_ms':[], 'explicit':[], 'track_id':[], 'album_id':[], 'track_number':[], 'key':[],
                           'mode':[], 'time_signature':[], 'acousticness':[], 'danceability':[], 'energy':[],
                           'instrumentalness':[], 'liveness':[], 'loudness':[], 'speechiness':[], 'valence':[],
                           'tempo':[]}

        for offset in range(0, max_number_of_albums, limit):
            offset += initial_offset
            r = self.sp.search(q='tag:new', type='album', offset=offset, limit=limit)
            # print(json.dumps(r, indent=1)) # prints nicely a dict
            response_albums = self.format_albums(r, response_albums)
            response_artists = self.format_artists(r, response_artists)
            response_authorship = self.format_authorship(r, response_authorship)
            response_tracks = self.format_tracks_from_albums(r, response_tracks)

        return response_albums, response_artists, response_authorship, response_tracks

    def get_data_over_time_period(self, start_year, wildcards, end_year=None, max_number_of_albums=100000, limit=10,
                                  initial_offset=0, add_inline=True, resume=None):
        """
        This method returns albums, artists and tracks dicts released over a time period in years.
        :param initial_offset:
        :param max_number_of_albums:
        :param limit:
        :param type:
        :param start_year:
        :param end_year:
        :return:
        """

        response_albums = {'name':[], 'artist':[], 'artist_spotify_link':[], 'artist_spotify_id':[],
                           'release_date':[], 'album_id':[], 'album_type':[], 'total_tracks':[]}
        response_artists = {'name': [], 'artist_id': [], 'type':[], 'followers':[], 'popularity':[], 'uri':[]}
        response_authorship = {'artist_id': [], 'album_id':[]}
        response_tracks = {'name':[], 'duration_ms':[], 'explicit':[], 'track_id':[], 'album_id':[], 'track_number':[], 'key':[],
                           'mode':[], 'time_signature':[], 'acousticness':[], 'danceability':[], 'energy':[],
                           'instrumentalness':[], 'liveness':[], 'loudness':[], 'speechiness':[], 'valence':[],
                           'tempo':[]}
        print('*'*80)
        print('*'*80)
        print(f'Getting data from {start_year} to {end_year}')
        print('*'*80)

        if end_year is None:
            time_span = [start_year]
        else:
            time_span = range(start_year, end_year + 1, 1)
        for year in time_span:
            # handle the resume case
            if resume and year==resume['year']:
                wildcards_actual = resume['wildcards_truncated']
            else:
                wildcards_actual = wildcards
            for wildcard in wildcards_actual:
                print()
                print(f'now retrieving for {year} with wildcard {wildcard}')
                for offset in range(0, max_number_of_albums, limit):
                    print(f'\rretrieved {offset} albums', end='')
                    offset += initial_offset
                    try:
                        # TODO add each different wildcard from a series of wildcards saved on an external file.
                        r = self.sp.search(q=wildcard + ' and year:' + str(year), type='album', offset=offset, limit=limit)
                    except spotipy.exceptions.SpotifyException as e:
                        print(e)
                        print('Probably end of the catalogue reached....')
                        break
                    except requests.exceptions.ReadTimeout as e:
                        print(e)
                        print('#'*50)
                        print('Timeout occurred, retrying in 2 minutes')
                        time.sleep(60)
                        r = self.sp.search(q=wildcard + ' and year:' + str(year), type='album', offset=offset, limit=limit)
                        print('#'*50)
                # print(json.dumps(r, indent=1)) # prints nicely a dict
                    response_albums = self.format_albums(r, response_albums)
                    response_artists = self.format_artists(r, response_artists)
                    response_authorship = self.format_authorship(r, response_authorship)
                    response_tracks = self.format_tracks_from_albums(r, response_tracks)
                    if add_inline:
                        try:
                            self.db.insert_values_from_dict('Albums', response_albums)
                            self.db.insert_values_from_dict('Artists', response_artists)
                            self.db.insert_values_from_dict('Authorship', response_authorship)
                            self.db.insert_values_from_dict('Tracks', response_tracks)
                        except IndexError:
                            print('No more albums to retrieve, moving to next task \n')
                        self.db.cnx.commit()
                        del response_albums, response_artists, response_tracks, response_authorship
                        response_albums = {'name':[], 'artist':[], 'artist_spotify_link':[], 'artist_spotify_id':[],
                                           'release_date':[], 'album_id':[], 'album_type':[], 'total_tracks':[]}
                        response_artists = {'name': [], 'artist_id': [], 'type':[], 'followers':[], 'popularity':[], 'uri':[]}
                        response_authorship = {'artist_id': [], 'album_id':[]}
                        response_tracks = {'name':[], 'duration_ms':[], 'explicit':[], 'track_id':[], 'album_id':[], 'track_number':[], 'key':[],
                                           'mode':[], 'time_signature':[], 'acousticness':[], 'danceability':[], 'energy':[],
                                           'instrumentalness':[], 'liveness':[], 'loudness':[], 'speechiness':[], 'valence':[],
                                           'tempo':[]}
        return response_albums, response_artists, response_authorship, response_tracks


    def format_artists(self, r, response_artists):
        for i in r['albums']['items']:
            artists = [self.sp.artist(a['id']) for a in i['artists']]
            response_artists['name'].extend([artist['name'] for artist in artists])
            response_artists['artist_id'].extend([artist['id'] for artist in artists])
            response_artists['uri'].extend([artist['uri'] for artist in artists])
            response_artists['popularity'].extend([artist['popularity'] for artist in artists])
            response_artists['followers'].extend([artist['followers']['total'] for artist in artists])
            response_artists['type'].extend([artist['type'] for artist in artists])

        return response_artists

    @staticmethod
    def format_albums(r, response_albums):
        response_albums['name'].extend([i['name'] for i in r['albums']['items']])
        response_albums['album_id'].extend([i['id'] for i in r['albums']['items']])
        formatted_release_dates = [i['release_date'] if '-' in i['release_date'] else i['release_date'] + '-01-01'
                                   for i in r['albums']['items']]
        response_albums['release_date'].extend(formatted_release_dates)
        print()
        print('release dates: ', formatted_release_dates)
        response_albums['album_type'].extend([i['album_type'] for i in r['albums']['items']])
        response_albums['total_tracks'].extend([int(i['total_tracks']) for i in r['albums']['items']])
        response_albums['artist'].extend([[i['artists'][j]['name']
                                           for j in range(len(i['artists']))] for i in r['albums']['items']])
        response_albums['artist_spotify_id'].extend([[i['artists'][j]['id'] for j in range(len(i['artists']))]
                                                     for i in r['albums']['items']])
        response_albums['artist_spotify_link'].extend([[i['artists'][j]['href'] for j in range(len(i['artists']))]
                                                       for i in r['albums']['items']])

        return response_albums

    def format_authorship(self, r, response_authorship):
        for i in r['albums']['items']:
            artists = [self.sp.artist(a['id']) for a in i['artists']]
            for artist in artists:
                response_authorship['album_id'].extend([i['id']])
                response_authorship['artist_id'].extend([artist['id']])
        return response_authorship

    def format_tracks_from_albums(self, r, response_tracks):
        for i in r['albums']['items']:
            tracks = self.get_tracks_from_album(album_id=i['id'])
            response_tracks = self.format_tracks(tracks, response_tracks, i['id'])
        return response_tracks

    def format_tracks(self, tracks, response_tracks, album_id=None):
        track_ids = [track['id'] for track in tracks['items']]
        response_tracks['name'].extend([track['name'] for track in tracks['items']])
        response_tracks['duration_ms'].extend([track['duration_ms'] for track in tracks['items']])
        response_tracks['explicit'].extend([track['explicit'] for track in tracks['items']])
        response_tracks['track_id'].extend(track_ids)
        response_tracks['track_number'].extend([track['track_number'] for track in tracks['items']])
        response_tracks['album_id'].extend([album_id for track in tracks['items']])
        # audio features
        audio_features = self.sp.audio_features(track_ids)
        audio_features = [a if a else self.create_empty_audio_feat() for a in audio_features]
        response_tracks['key'].extend([track_feature['key'] for track_feature in audio_features])
        response_tracks['mode'].extend([track_feature['mode'] for track_feature in audio_features])
        response_tracks['time_signature'].extend([track_feature['time_signature'] for track_feature in audio_features])
        response_tracks['acousticness'].extend([track_feature['acousticness'] for track_feature in audio_features])
        response_tracks['danceability'].extend([track_feature['danceability'] for track_feature in audio_features])
        response_tracks['energy'].extend([track_feature['energy'] for track_feature in audio_features])
        response_tracks['instrumentalness'].extend([track_feature['instrumentalness'] for track_feature in audio_features])
        response_tracks['liveness'].extend([track_feature['liveness'] for track_feature in audio_features])
        response_tracks['loudness'].extend([track_feature['loudness'] for track_feature in audio_features])
        response_tracks['speechiness'].extend([track_feature['speechiness'] for track_feature in audio_features])
        response_tracks['valence'].extend([track_feature['valence'] for track_feature in audio_features])
        response_tracks['tempo'].extend([track_feature['tempo'] for track_feature in audio_features])

        return response_tracks

    @staticmethod
    def create_empty_audio_feat():
        return {
            'key':'', 'mode':'', 'time_signature':'', 'acousticness':'', 'danceability':'', 'energy':'',
            'instrumentalness':'', 'liveness':'', 'loudness':'', 'speechiness':'', 'valence':'', 'tempo':''
        }

    def get_tracks_from_album(self, album_id='6M4Nu5UgX097dxeF2lm9P8'):
        r = self.sp.album_tracks(album_id)
        return r

    @staticmethod
    def save_response(response, filename):
        response_df = pd.DataFrame(response)
        response_df = response_df.drop_duplicates()
        response_df.to_csv(filename)

    def get_featured_new_releases(self):
        for j in range(4):
            response = self.sp.new_releases(limit=50, offset=0, country='IT')
            while response:
                albums = response['albums']
                print(len(albums))
                for i, item in enumerate(albums['items']):
                    print(albums['offset'] + i, item['name'], [a['name'] for a in item['artists']])

                if albums['next']:
                    response = self.sp.next(albums)
                else:
                    response = None


    #####################################################################################
    #####################################################################################
    #####################################################################################
    def artist_genre(self, genre):
        """Download all artists info from a genre """

        artist=[]
        at=self.sp.search(q='genre:' + genre,limit=50, type='artist')['artists']

        if at['total']>50:
            for i in range(0,at['total'],50):
                artist+=self.sp.search(q='genre:'+ genre,limit=50,type='artist',offset=i)['artists']['items']
        save_json("../../data/artists/{}_artists_info".format(genre),artist)
        print("Download {}'s artists info".format(genre))

        return artist


    def album_artists(self):
        """Download all albums info from artists """
        artist=open_json("data/artists/{}_artists_info".format(self.genre))
        album={}

        for i in artist:
            name=i['name']
            album[name]=[]
            ab=self.sp.artist_albums(i['id'], limit=50)

            if ab['total']>50:
                for j in range(0,ab['total'],50):
                    album[name]+=self.sp.artist_albums(i['id'], limit=50,offset=j)['items']
            else:
                album[name]+=self.sp.artist_albums(i['id'], limit=50)['items']
            print("Download {}'s albums info".format(name))
        save_json("data/albums/{}_albums_info".format(self.genre),album)

        return album


    def songs_albums(self):
        """Download all songs info from albums """
        album=open_json("data/albums/{}_albums_info".format(self.genre))

        songs={}

        for k,v in album.items():
            list_dir=os.listdir('./data/tracks/')
            k=k.replace('/','_')

            if "{}_tracks_info.json".format(k) not in list_dir:
                songs['artist']=k
                songs['albums']={}

                for i in v:
                    songs['albums'][i['id']]=self.sp.album_tracks(i['id'])['items']
                save_json("data/tracks/{}_tracks_info".format(k),songs)
                print("Download {}'s tracks info".format(k))

        return songs


    def songs_albums_add_popularity(self):
        list_dir=os.listdir('./data/tracks/')

        for fl in list_dir:
            try:
                album=open_json("data/tracks/{}".format(fl[:-5]))
            except:
                pass
            for ak,av in album['albums'].items():
                for tr in av:
                    try:
                        tr['popularity']=self.sp.track(tr['id'])['popularity']
                    except Exception as e:
                        print(e,album['artist'])
            print("Add track popularity to {}'s songs".format(album['artist']))
            save_json("data/tracks/{}".format(fl[:-5]),album)


    def audio_feature(self,artist):
        """Download audio features of all tracks of one artist

        :artist(string)

        """

        albums=open_json('data/tracks/{}_tracks_info'.format(artist))['albums']
        Analysis=pd.DataFrame()
        for k,v in albums.items():
            audios=[]
            track_name=[]
            for i in v:
                audios.append(i['id'])
                track_name.append(i['name'])
            analysis=pd.DataFrame(self.sp.audio_features(audios))

            analysis['track_name']=track_name
            analysis['album']=self.sp.album(k)['name']
            analysis['release_date']=self.sp.album(k)['release_date']
            analysis['album_id']=k
            analysis['artist']=artist
            Analysis=Analysis.append(analysis)

        Analysis=Analysis.drop_duplicates('id')
        Analysis=Analysis[~Analysis.analysis_url.isnull()]

        if 0 in Analysis.columns:
            del Analysis[0]

        # drop similar songs, highly possible the same songs
        Analysis=Analysis.drop_duplicates(['acousticness','liveness',
                                           'instrumentalness','speechiness',
                                           'danceability','energy','valence'])

        print("Extract tracks analysis of artist {}".format(artist))

        Analysis.to_hdf("./data/analysis/{}_tracks_analysis.h5".format(artist),'analysis')

    def audio_feature_all(self):
        """Download audio features of all tracks of all artists of one genre

        """
        dir_l=os.listdir('./data/tracks/')
        for artist in dir_l:
            if artist[:-17]+'_tracks_analysis.h5' not in os.listdir('./data/analysis/'):
                try:
                    self.audio_feature(artist[:-17])
                except Exception as e:
                    print("Artist {} has something wrong:{}".format(artist,e))

# @Created on  : 28/10/2020 18:55
# @Author      : Jacopo Solari
# @Email       : jaco.solari@gmail.com
# @File        : DatabaseManger.py

import mysql.connector
from mysql.connector import errorcode
from mysql.connector.errors import ProgrammingError
import pandas as pd
import os


class DataBaseManager():
    """
    This class can handle all the operations on a database.
    """

    def __init__(self, db_name='myspotify', logging_level=0):
        self.DB_NAME = db_name
        print(f'Connecting to {self.DB_NAME} database..')
        try:
            self.cnx = mysql.connector.connect(user='root', password='Jacopo87',
                                               host='localhost',
                                               database='myspotify')
            self.cursor = self.cnx.cursor()
            print(f'Connected to {self.DB_NAME}!')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print(f"Database \"{self.DB_NAME}\" does not exist")
            else:
                print(err)
        self.logging_level = logging_level
        self.query = None
        self.args = None

    def create_tables(self, tables):
        """
        Create a new table in the database
        :param tables: dict, the keys are the name of the tables and the values contains
                the actual query strings needed to create the tables.
        """
        for table in tables.keys():
            print(f'Creating {table} table in {self.DB_NAME}..', end='')
            try:
                self.cursor.execute(tables[table])
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("\nERROR: Table already exists.")
                else:
                    print(err.msg)
            else:
                print("Done")

    def drop_table(self, table, confirm=False):
        """
        Drops a table from the database to which the DatabaseManager is connected.
        :param table: str, the name of the table to drop.
        :param confirm: bool, if True asks for user confirmation of deletion.
        """
        if confirm:
            confirmation = input(f'Are you sure you want to delete the table \'{table}\'?')
        else:
            confirmation = 'y'
        if confirmation == 'yes' or confirmation[0] == 'y':
            try:
                self.cursor.execute(f'drop table {table}')
                print(f'Deleting {table} table from {self.DB_NAME}..')
            except ProgrammingError:
                print(f' The table \'{table}\' does not exist')
            else:
                return

    def insert_value(self, table, columns, values, ignore_duplicates=True):
        """
        Insert a row into the database.
        The method, to be versatile, needs to parse lists and tuples arguments into a query string.
        :param ignore_duplicates:  bool
        :param table:  str, name of the table
        :param columns: tuple, tuple of strings corresponding to the columns to insert data in
        :param values: tuple, tuple of mixed types containing the values to add
        """
        values = self.input_check_for_values(values)
        if self.logging_level > 0:
            print(f'Values: {values}')
            print(f'Columns: {columns}')
        columns = self.remove_chars_from_string(str(columns), '\'')  # removing string apostrophe
        if '(' not in columns:  # handles the case of single element
            columns = '(' + columns + ')'

        values_types = self.create_tuple_of_placeholders(values)
        if len(values_types) == 1:
            values_types = self.remove_chars_from_string(
                str(values_types), to_remove=[',', '\''])  # removing trailing comma
        else:
            values_types = self.remove_chars_from_string(str(values_types), to_remove='\'')
        # execute MySql statement
        self.query = f'insert ignore into {table} ' + str(columns) + f' values {values_types}'
        if not ignore_duplicates:
            self.query = self.remove_chars_from_string(self.query, 'ignore')

        self.cursor.execute(self.query, values)

    def insert_values_from_file(self, table, filepath, primary_key):
        """
        Inserts values read from a file into a table.
        The columns where the values are inserted are the columns that are
        both in the file (e.g. csv) and the table we want to insert into.
        :param table: str, name of the table
        :param filepath: str, path (relative to the project) of the file containing the data to insert
        :param primary_key: str, name of the primary key in the table. It has to be also in the file.
        """
        df = pd.read_csv(filepath)
        if primary_key not in list(df.columns):
            print(f'ERROR: the primary key {primary_key} is not a column of the file '
                  f'from which the data is supposed to be inserted in the table {table}')
        df = df.drop_duplicates(subset=primary_key)
        file_columns = df.columns
        columns = self.get_columns_names(table)
        columns_in_common = set(columns).intersection(set(file_columns))
        if self.logging_level > 0:
            print(f'Adding values from {filepath.split(os.sep)[-1]} from these columns: \n   {columns_in_common}')
        df = df[columns_in_common]
        for i in range(len(df)):
            values = list(df.loc[i].values)
            self.insert_value(table=table,
                              columns=tuple(columns_in_common),
                              values=values)
            self.cnx.commit()

    def get_columns_names(self, table):
        """
        Get all the column names from a table into a list.
        :param table: str, table name
        :return: list, containing all column names from table
        """
        self.cursor.execute(f"SHOW columns FROM {table}")
        return [column[0] for column in self.cursor.fetchall()]

    def insert_values_from_dict(self, table, d, ignore_duplicates=True):
        """
        This method inserts values from the dictionary.
        The keys are the columns to insert and the values is the list of values to insert.
        :param table: str
        :param ignore_duplicates: bool
        :param d: dict
        """
        columns = list(d.keys())
        table_columns = self.get_columns_names(table)
        columns_in_common = set(table_columns).intersection(set(columns))
        values = pd.DataFrame(d).fillna(0)[list(columns_in_common)].values
        if self.logging_level > 0:
            print(f'columns in common between table {table} and dictionary to insert : {columns_in_common}')
        if len(list(columns_in_common))==1:  # handles the case of single element
            columns_in_common = self.remove_chars_from_string(str(tuple(columns_in_common)), ['\'', ','])
        else:
            columns_in_common = self.remove_chars_from_string(str(tuple(columns_in_common)), '\'')  # removing string apostrophe
        values_types = self.create_tuple_of_placeholders(values[0])
        if len(values_types) == 1:
            values_types = self.remove_chars_from_string(
                str(values_types), to_remove=[',', '\''])  # removing trailing comma
        else:
            values_types = self.remove_chars_from_string(str(values_types), to_remove='\'')
        self.query = 'INSERT ignore INTO ' + table + ' ' + str(columns_in_common) + ' VALUES ' + values_types
        if self.logging_level > 0:
            print(self.query)
        if not ignore_duplicates:
            self.query = self.remove_chars_from_string(self.query, 'ignore')
        self.cursor.executemany(self.query, [tuple(v) for v in values])

    def select(self, columns):
        self.query = 'SELECT ' + self.remove_chars_from_string(str(columns), ['\'', '\"'])
        return self

    def fromm(self, table):
        self.query = self.query + ' FROM ' + self.remove_chars_from_string(str(table), ['\'', '\"'])
        return self

    def where(self, condition, args):
        self.query = self.query + ' WHERE ' + self.remove_chars_from_string(str(condition), ['\'', '\"'])
        self.args = args
        return self

    def run_query(self, verbose=True):
        self.query = self.query + ';'
        if self.logging_level > 0:
            print(self.query)
        self.cursor.execute(self.query, self.args)
        if verbose:
            for col in self.cursor:
                print(col)

    @staticmethod
    def create_tuple_of_placeholders(values):
        """
        This function matches each object type in values to the key of the type_dict
        :param values: list, list of objects.
        :return: tuple, containing the identifiers for each type to insert.
        """
        return tuple("%s" for val in values)
        # return tuple("%s") * len(values)

    @staticmethod
    def remove_chars_from_string(s, to_remove):
        if type(to_remove) != list:
            to_remove = [to_remove]
        for character in to_remove:
            s = s.replace(character, '')
        return s

    def input_check_for_values(self, values):
        if self.logging_level > 0:
            print(values)
        v2 = []
        for v in values:
            try:
                v2.append(int(v))
            except ValueError:
                v2.append(v)
        return v2





def update_data(genre='k-pop'):
    spf=SpotifyAPI(genre)
    #upate artists info
    artist=spf.artist_genre()
    #update albums info of artists
    album=spf.album_artists(artist)
    #update songs info of albums
    spf.songs_albums(album)


def save_json(name,dt):
    with open('./%s.json'%name,'w') as f:
        json.dump(dt,f)


def open_json(name):
    with open('./%s.json'%name,'r') as f:
        dt=json.load(f)
    return dt


if __name__=="__main__":

    #spotf=MySpotify()
    #spotf.audio_feature_all()

    pass