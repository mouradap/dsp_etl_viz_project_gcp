import os
import logging
from google.cloud import storage

class GCSHandle():
    def __init__(self):
        self.gcs = storage.Client()

    def set_bucket(self, bucket):
        self.bucket_name = bucket
        self.bucket = self.gcs.bucket(self.bucket_name)
    
    def create_bucket(self, bucket_name):
        try:
            self.bucket = self.gcs.create_bucket(bucket_name)
            print("Bucket {0} created.".format(bucket_name))
        except Exception as e:
            print("Failed creating bucket {0}. Error: {1}".format(bucket_name, e))

    def delete_file(self, filename):
        obj = self.bucket.blob(filename)
        try:
            obj.delete()
        except Exception as e:
            print('Failed deleting object {0}. \n Error: {1}'.format(blob, e))

    def download_file(self, source_filename, destination_filename):
        obj = self.bucket.blob(source_filename)
        try:
            obj.download_to_filename(destination_filename)
            print("Object {0} has been downloaded!".format(source_filename))
        except Exception as e:
            print("Failed downloading object {0}. Reason: {1}".format(source_filename, e))

    def move_file(self, filename, new_name):
        obj = self.bucket.blob(filename)
        try:
            new_obj = self.bucket.rename_blob(obj, new_name)
            print("Object {0} has been moved to {1}".format(filename, new_name))
        except Exception as e:
            print("Failed moving object {0}. Reason: {1}".format(filename, e))

    def upload_file(self, source_filename, destination_filename):
        obj = self.bucket.blob(destination_filename)
        try:
            obj.upload_from_filename(source_filename)
            print("File {0} uploaded to {1}".format(source_filename, destination_filename))
        except Exception as e:
            print("Failed uploading file {0}. Reason: {1}".format(source_filename, e))
    
if __name__ == '__main__':
    gcs = GCSHandle()
    # gcs.create_bucket('dsp_project')
    gcs.set_bucket('dsp_project')
    gcs.upload_file('./test/gcs_test.txt', 'handle_test/gcs_test.txt')
    gcs.move_file('handle_test/gcs_test.txt', 'handle_test/new_gcs_test.txt')
    gcs.download_file('handle_test/new_gcs_test.txt', './test/new_gcs_test.txt')




    # def _auth():
    #     pass

    # def __get_bucket(self):
    #     self.bucket_name = os.environ.get('BUCKET_NAME',
    #                                     app_identity.get_default_gcs_bucket_name())
        
    #     self.response.headers['Content-Type'] = 'text/plain'
    #     self.respose.write('Connecting to bucket: {0}...'.format(self.bucket_name))

    # def list_bucket(self, bucket):
    #     self.response.write('Reading files from bucket {0}'.format(bucket))

    #     page_size = 1
    #     stats = gcs.listbucket(bucket, max_keys=page_size)
    #     while True:
    #         count = 0
    #         for stat in stats:
    #             count += 1
    #             self.response.write(repr(stat))
    #             self.response.write('\n')
            
    #         if count != page_size or count == 0:
    #             break
    #         stats = gcs.listbucket(bucket, max_keys=page_size, marker=stat.filename)
    
    # def create_file(self, filename):
        
    #     self.response.write('Creating file {0} in {1} bucket.'.format(filename, self.bucket_name))

    #     write_retry_params = gcs.RetryParams(backoff_factor = 1.1)
    #     gcs_file = gcs.open(
    #                         filename,
    #                         'w',
    #                         content_type='text/plain',
    #                         retry_params = write_retry_params
    #                         )
    #     self.tmp_filenames_to_clean_up.append(filename)

    # def read_file(self, filename):
    #     self.response.write('Reading contents of {0}'.format(filename))
    #     gcs_file = gcs.open(filename)
    #     contents = gcs_file.read()
    #     gcs_file.close()
    #     self.response.write(contents)
    
    # def _delete_temp_files(self):
    #     self.response.write("Cleaning up temporary files...")

    #     for filename in self.tmp_filenames_to_clean_up:
    #         self.response.write("Deleting file {0}\n".format(filename))
    #         try:
    #             gcs.delete(filename)
    #         except gcs.NotFoundError as e:
    #             logging.error(e)
    
    # def delete_file(self, filename):
    #     self.response.write("Deleting file {0}".format(filename))
    #     try:
    #         gcs.delete(filename)
    #     except gcs.FileNotFoundError as e:
    #         logging.error(e)