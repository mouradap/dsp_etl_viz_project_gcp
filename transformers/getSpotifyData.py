import pandas as pd
from datetime import datetime
import sys
import logging
import os


class SpotifyToGCS:
    def __init__(self, SpotifyHandle, GCSHandle):
        self.sp = SpotifyHandle
        self.gcs = GCSHandle
        self.tmp_files_path = os.getcwd() + "/dags/data/"
        print(self.tmp_files_path)

    def download_tracks_data(self, **kwargs):
        filename = "query_%s_%s.csv"
        now = datetime.now().strftime("%Y-%m-%d@%H:%M:%S")
        print(kwargs)
        if "year" in kwargs.keys():
            try:
                res = self.sp.search_year(year=kwargs["year"], limit=kwargs["limit"])
            except Exception as e:
                logging.error(e)
            filename = filename % (kwargs["year"], now)
        elif "artist" in kwargs.keys():
            try:
                res = self.sp.search_artist(
                    artist=kwargs["artist"], limit=kwargs["limit"]
                )
            except Exception as e:
                logging.error(e)
            filename = filename % (kwargs["artist"], now)
        else:
            print("You should inform a year or artist as request type.")
        df = pd.DataFrame(res)
        # print(df)
        filepath = self.tmp_files_path + filename
        df.to_csv(filepath)
        print("Records saved at {}!".format(filepath))

        return filename

    def load_to_gcs(self, bucket_name, source_path, dest_path):
        self.gcs.set_bucket(bucket_name)
        self.gcs.upload_file(source_path, dest_path)

    def run(self, type: str, value: str, limit: int, bucket: str):
        if type == "year":
            downloaded_filename = self.download_tracks_data(year=value, limit=limit)
            source_file = self.tmp_files_path + downloaded_filename
            dest_file = "all/transient/{}".format(downloaded_filename)
            self.load_to_gcs(bucket, source_file, dest_file)

        elif type == "artist":
            downloaded_filename = self.download_tracks_data(artist=value, limit=limit)
            source_file = self.tmp_files_path + downloaded_filename
            dest_file = "{0}/transient/{1}".format(value, downloaded_filename)
            self.load_to_gcs(bucket, source_file, dest_file)


if __name__ == "__main__":
    import sys

    sys.path.append("..")
    from handles.googleCloudStorageHandle import GCSHandle
    from handles.spotifyHandle import SpotifyHandle

    gcs = GCSHandle()
    sphandle = SpotifyHandle()
    sp = SpotifyToGCS(sphandle, gcs)
    year = 2021
    # sp.download_tracks_data(year = year, limit = 50)
    sp.download_tracks_data(year="2021", limit=1)
    # sp.run(
    #     type = 'artist',
    #     value = 'Opeth',
    #     limit = 100,
    #     bucket = 'dsp_project'
    # )
