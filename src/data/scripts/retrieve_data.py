# @Created on  : 27/10/2020 16:46
# @Author      : Jacopo Solari
# @Email       : jaco.solari@gmail.com
# @File        : retrieve_data.py

"""
This script will create the needed object to obtain the raw data (samples and category) via the Splice API
"""

import SpotifyAPI

sp = SpotifyAPI.SpotifyAPI()

# sp.get_featured_new_releases()
sp.get_newly_released_albums(max_number_of_albums=200)
# sp.artist_genre('k-pop')
