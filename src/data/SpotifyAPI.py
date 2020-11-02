# @Created on  : 28/10/2020 01:20
# @Author      : Jacopo Solari
# @Email       : jaco.solari@gmail.com
# @File        : SpotifyAPI.py

import os
import spotipy
import json
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
import mysql

from DatabaseManger import DataBaseManager


class SpotifyAPI(object):
    """This class can download data from Spotify API.
    """
    def __init__(self):
        self.authenticate()

    def authenticate(self):
        my_id = os.environ.get('CLIENT_ID')
        secret_key = os.environ.get('SECRET_KEY')
        client_credentials_manager = SpotifyClientCredentials(client_id=my_id, client_secret=secret_key)
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    def list_all_genres(self):
        pass

    def get_newly_released_albums(self, max_number_of_albums=20000, limit=10):
        response = {'name':[], 'artist':[], 'artist_spotify_link':[], 'artist_spotify_id':[],
                    'release_date':[], 'album_id':[], 'album_type':[], 'total_tracks':[]}
        for offset in range(0, max_number_of_albums, limit):
            r = self.sp.search(q='tag:new', type='album', offset=offset, limit=limit)
            response['name'].extend([i['name'] for i in r['albums']['items']])
            response['release_date'].extend([i['release_date'] for i in r['albums']['items']])
            response['album_type'].extend([i['type'] for i in r['albums']['items']])
            response['album_id'].extend([i['id'] for i in r['albums']['items']])
            response['total_tracks'].extend([int(i['total_tracks']) for i in r['albums']['items']])
            response['artist'].extend([[i['artists'][j]['name']
                                        for j in range(len(i['artists']))] for i in r['albums']['items']])
            response['artist_spotify_id'].extend([[i['artists'][j]['id'] for j in range(len(i['artists']))]
                                                  for i in r['albums']['items']])
            response['artist_spotify_link'].extend([[i['artists'][j]['href'] for j in range(len(i['artists']))]
                                                    for i in r['albums']['items']])


        db = DataBaseManager(logging_level=1)

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

        r = {key: value for key, value in response.items() if key in ['album_id', 'name', 'total_tracks', 'album_type', 'release_date']}

        db.insert_values_from_dict('Albums', r)
        db.cnx.commit()

        responsedf = pd.DataFrame(response)
        responsedf['total_tracks'] = responsedf['total_tracks'].astype(int)
        responsedf = responsedf.drop_duplicates(subset=['album_id'])
        print(os.environ.get('IDE_PROJECT_ROOTS'))
        responsedf.to_csv('../../resources/data/trial.csv')
        # print(json.dumps(response, indent=1)) # prints nicely a dict
        print(responsedf['album_id'])


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