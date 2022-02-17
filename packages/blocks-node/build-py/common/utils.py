import pyspark.files
import zipfile
import yaml
from typing import Dict, List, Any, Tuple, Pattern, Match, Optional, Set
import itertools
import json
import logging
import boto3
import re
import os

logger = logging.getLogger("dy_runner")


def get_file_locations(folder, path, limit=1, sort='last_modified', ascending=True) -> List[str]:
    """
        loads either from local FS or S3, searching for the file by regexp. returns first found
        folder - input folder. prefixed with s3:// if s3 otherwise local
        path - regexp with path info
        limit - max number of files to fetch. use None to load all files
        sort - if files are limited, how to sort? will return the first from the list based on the sorting. default 'last_modified'. possible values - 'last_modified' | 'size'
        ascending - if files are limited, how to sort? will return the first from the list based on the sorting

    Returns
        files to process.
        all files found in folder matching regexp
    """
    match_file = re.compile(os.path.basename(path), flags=re.IGNORECASE).match
    files_list: List[str] = []
    if folder.startswith("s3"):
        # load from s3
        parsedFolder = folder.split("://")
        s3 = boto3.resource('s3')
        # Bucket to use
        bucket = s3.Bucket(parsedFolder[1].split("/")[0])

        # list objects within a given prefix
        path_without_filename: str = '/'.join(parsedFolder[1].split('/')[1:])
        bucket_objects = bucket.objects.filter(Delimiter='/', Prefix=path_without_filename)
        # list files sorted by sort order

        def get_s3_obj_property(obj, property):
            if property == 'last_modified':
                return obj.last_modified
            elif property == 'size':
                return obj.size
            else:
                return obj.key

        files_list = [obj.key.split("/")[-1]
                      for obj in sorted(
                          bucket_objects, key=lambda x: get_s3_obj_property(x, sort),
                          reverse=(not ascending))]
    else:
        # load from file system
        files_list = os.listdir(folder)
        # we add full path to get properties of the file
        full_path_files = [os.path.join(folder, f) for f in files_list]

        def get_file_obj_property(obj: str, property: str):
            if property == 'last_modified':
                return os.path.getmtime(obj)
            elif property == 'size':
                return os.path.getsize(obj)
            else:
                return obj

        full_path_files.sort(key=lambda x: get_file_obj_property(x, sort), reverse=(not ascending))
        # strip the folder name
        files_list = [os.path.basename(filename) for filename in full_path_files]

    # filter by regexp
    files = list(filter(match_file, files_list))

    if (len(files) > 0):
        all_matched_files = [os.path.join(folder, filename) for filename in files]
        logger.debug("found matching flat files")
        logger.debug(all_matched_files[0:limit])
        return all_matched_files[0:limit]
    else:
        raise FileNotFoundError("can't find input file %s at %s" % (path, folder))


def get_catalog(source: str) -> Dict[str, Any]:
    return get_source(os.path.join(pyspark.files.SparkFiles.getRootDirectory(), "catalog.zip"), source)


def get_source(catalog, source: str) -> Dict[str, Any]:
    catalog_zip = zipfile.ZipFile(catalog)
    source_parts = source.split(".")
    module = source_parts[0]
    source_name = source_parts[1]
    with catalog_zip.open(module+".yaml") as yamlfile:
        catalog_entries = yaml.safe_load(yamlfile)
        for entry in catalog_entries:
            if entry["id"] == source_name:
                return entry
    raise ValueError(f"no catalog entry found for {source}")


def get_datafolder(env, foldertype) -> str:
    env_defaults = {
        "raw": "/opt/dy/data"
    }
    datafolder = env.get("datafolders", {}).get(foldertype, env_defaults.get(foldertype))
    if datafolder:
        return datafolder
    else:
        raise ValueError(f"no datafolder found for {foldertype}")


def get_connection(env, connection_name: str) -> Dict[str, Any]:
    if connection_name in env["connections"]:
        return env["connections"][connection_name]
    else:
        raise ValueError(f"no connection entry found for {connection_name}")
