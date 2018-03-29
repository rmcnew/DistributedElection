import logging

import boto3

from shared.constants import *


class SimpleStorageService:
    """Wrapper class for easy AWS Simple Storage Service (S3) usage"""

    def __init__(self):
        self.s3 = boto3.resource(S3)
        self.s3client = boto3.client(S3)
        self.lfde_bucket = self.s3.Bucket(LFDE_S3_BUCKET)

    def list_folder_contents(self, prefix):
        keys = []
        response = self.s3client.list_objects_v2(Bucket=LFDE_S3_BUCKET, Prefix=prefix)
        if CONTENTS in response:
            contents = response[CONTENTS]
            for obj in contents:
                keys.append(obj[KEY])
        return keys

    def list_input_folder_contents(self):
        keys = self.list_folder_contents(LFDE_S3_INPUT_FOLDER_PREFIX)
        logging.debug("{} contents: {}".format(LFDE_S3_INPUT_FOLDER, keys))
        return keys

    def download_string_pair(self, string_pair_key, local_save_path):
        logging.debug("Downloading {} to {}".format(string_pair_key, local_save_path))
        self.lfde_bucket.download_file(string_pair_key, local_save_path)

    def list_output_folder_contents(self):
        keys = self.list_folder_contents(LFDE_S3_OUTPUT_FOLDER)
        logging.debug("{} contents: {}".format(LFDE_S3_OUTPUT_FOLDER, keys))
        return keys

    def upload_string_pair_result(self, string_pair_result_filename, local_result_path):
        upload_target = "{}/{}".format(LFDE_S3_OUTPUT_FOLDER, string_pair_result_filename)
        logging.debug("Uploading {} to {}".format(local_result_path, upload_target))
        self.lfde_bucket.upload_file(local_result_path, upload_target)
