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

    def get_new_releases(self, max_number_of_albums=20000, limit=10, initial_offset=0, filename='../../resources/data/trial.csv'):
        """
        This method downloads all the information about new releases.
        It stores the informations in 3 different dictionaries: albums, artists and authorship.
        These dict can easily be added to the spotify MySQL database.
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

        self.save_response(response=response_albums, filename=filename)
        self.save_response(response=response_artists, filename='../resources/data/trial_artists.csv')
        # print(json.dumps(response_albums, indent=1)) # prints nicely a dict
        return response_albums, response_artists, response_authorship, response_tracks

    def get_data_over_time_period(self, type, start_year, end_year=None, max_number_of_albums=20000, limit=10,
                                  initial_offset=0):
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
        pass

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
        response_albums['release_date'].extend([i['release_date'] for i in r['albums']['items']])
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

    def get_tracks_from_album(self, album_id='6M4Nu5UgX097dxeF2lm9P8'):
        r = self.sp.album_tracks(album_id)
        return r


    def save_response(self, response, filename):
        responsedf = pd.DataFrame(response)
        # responsedf = responsedf.drop_duplicates(subset=['album_id'])
        responsedf.to_csv(filename)

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