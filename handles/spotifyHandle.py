import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


class SpotifyHandle:
    def __init__(self):
        self.__clientid = os.environ.get("SPOTIFY_CLIENT_ID")
        self.__clientsecret = os.environ.get("SPOTIFY_CLIENT_SECRET")
        self.__redirecturi = os.environ.get("SPOTIPY_REDIRECT_URI")
        self._client_manager = SpotifyClientCredentials(
            client_id=self.__clientid,
            client_secret=self.__clientsecret,
        )
        self.sp = spotipy.Spotify(client_credentials_manager=self._client_manager)
        # self.base_url = "https://spotifycharts.com/regional/%s/download"

    def search_artist(self, artist, limit=50):
        artist_name = []
        track_name = []
        track_album = []
        popularity = []
        track_duration = []
        track_id = []

        results = self.sp.search(q=artist)
        for track in results["tracks"]["items"]:
            track_name.append(track["name"])
            popularity.append(track["popularity"])
            track_duration.append(track["duration_ms"])
            track_album.append(track["album"]["name"])
            track_id.append(track["id"])
            artist_name.append(artist)

        response_obj = {
            "artist_name": artist_name,
            "track_album": track_album,
            "track_name": track_name,
            "track_id": track_id,
            "track_popularity": popularity,
            "track_duration": track_duration,
        }

        return response_obj

    def search_year(self, year, limit=50):
        artist_name = []
        track_name = []
        popularity = []
        track_id = []
        track_type = []
        track_duration = []
        genre = []

        results = self.sp.search(q="year:{}".format(year), limit=limit)
        # print(results['tracks']['items'][0].keys())
        for item in results["tracks"]["items"]:
            artist_name.append(item["artists"][0]["name"])
            track_name.append(item["name"])
            track_id.append(item["id"])
            popularity.append(int(item["popularity"]))
            track_type.append(item["type"])
            track_duration.append(int(item["duration_ms"]))

        response_obj = {
            "artist_name": artist_name,
            "track_name": track_name,
            "track_id": track_id,
            "popularity": popularity,
            "track_type": track_type,
            "track_duration": track_duration,
        }

        return response_obj


if __name__ == "__main__":
    api = SpotifyHandle()
    # response_obj = api.get_sample_tracks()
    # print(response_obj)
    print(api.search_year("2021"))
    # print(api.search_artist('Opeth'))
    # print(api.search_date('2021-01-01'))
