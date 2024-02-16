import argparse
import io
import json
import os
import re
import gc
import sys
import s3fs
import boto3
import multiprocessing
from s2sphere import LatLng, CellId

import pandas as pd
from jinja2 import Template
from minio import Minio
from pyld import jsonld


def get_cell_id(longitude: float, latitude: float, level: int ) -> int:
    return CellId.from_lat_lng(LatLng.from_degrees(latitude, longitude)).parent(level).id()

def to_ld(row):
    # r = str("test {}".format(row['Type']))
    # branch from here to RDF builders.

    # calculate our only new value to add Y -> lat, X -> long
    s2cell18 = str(get_cell_id(row['X'], row['Y'], 18))
    s2cell13 = str(get_cell_id(row['X'], row['Y'], 13))

    # print("{}  {}  {}".format(row['X'], row['Y'], s2cell13))

    # Type Description Address AreaSqm X Y Z SOURCE_ID UFOKN_ID FEATURE_ID GEOID
    # make dict
    kwd = {"Type": row['Type'], "Description": row['Description'], "Address": row['Address'], "AreaSqm": row['AreaSqm'],
           "X": row['X'], "Y": row['Y'], "Z": row['Z'], 'SOURCE_ID': row['SOURCE_ID'],
           'UFOKN_ID': row['UFOKN_ID'], 'FEATURE_ID': row['FEATURE_ID'], 'GEOID': row['GEOID'], 'CellID13': s2cell13,
           'CellID18': s2cell18}
    # print("{} {} {}".format(kwd['X'], kwd['Y'], kwd['Z']))

    # read template and alter
    with open("template.json", "r") as file:
        template_str = file.read()

    template = Template(template_str)
    populated_json = template.render(kwd)

    nt = ""
    try:
        json_data = json.loads(populated_json)
        try:
            nt = jsonld.normalize(json_data, {'algorithm': 'URDNA2015', 'format': 'application/n-quads'})
        except Exception as e:
            print(e)
            # print(kwd)
            print("OFOKN_ID: {}".format(kwd['UFOKN_ID']))
    except Exception as e:
        print(e)
        # print(kwd)
        print("OFOKN_ID: {}".format(kwd['UFOKN_ID']))

    return nt


def parse_s3_url(s3_url):
    protocol, url = s3_url.split("://")
    if protocol != 's3':
        raise ValueError('URL is not a valid S3 URL')

    split_url = url.split("/")
    server_url = split_url[0]
    bucket_name = split_url[1]
    object_path = "/".join(split_url[2:])

    return server_url, bucket_name, object_path

def fips_from_path(path):
    string = path

    # Extract STATEFP and COUNTYFP using regular expressions
    # example url: s3://s3.amazonaws.com/backup.udp-data-urmi/STATEFP=56/COUNTYFP=021/data.parquet
    pattern = r"/STATEFP=(\d+)/COUNTYFP=(\d+)"
    match = re.search(pattern, string)

    state_fp = ""
    county_fp = ""
    if match:
        state_fp, county_fp = match.groups()
    else:
        print("Error: STATEFP and COUNTYFP not found in the string.")

    return state_fp, county_fp


def etl(etlargs):
    obj, u, b, odir = etlargs
    session = boto3.Session(profile_name='ufokn')
    s3 = session.client('s3')

    path = "s3://{}/{}/{}".format(u, b, obj.object_name)
    print("Processing: {}".format(path))  # Each object is a dictionary containing details like object name, size, etc.

    state_fp, county_fp = fips_from_path(path)

    buffer = io.BytesIO()
    s3.download_fileobj("backup.udp-data-urmi", "STATEFP={}/COUNTYFP={}/data.parquet".format(state_fp, county_fp),
                        buffer)
    df = pd.read_parquet(buffer)

    # Elevation Catch, run by group to ensure this isn't changing history
    df['Z'] = df['Z'].fillna(0)
    df['AreaSqm'] = df['AreaSqm'].fillna(0)

    # build the RDF for each row
    results = df.apply(to_ld, axis=1)
    rdf = pd.DataFrame(results, columns=['rdf'])
    rdf.to_parquet("{}/fip_{}{}.parquet".format(odir, state_fp, county_fp))

    del df
    del rdf
    s3.close()
    gc.collect()

# TODO  add @id to the json-ld tempate
def main():
    # Parameters and environment variable section
    parser = argparse.ArgumentParser(description="Process some arguments.")
    parser.add_argument("--source", type=str, help="Source URL")

    parser.add_argument("--outputdir", type=str, help="Directory for output files")

    args = parser.parse_args()
    if args.source is None:
        print("Error: the --source argument is required")
        sys.exit(1)
    if args.outputdir is None:
        print("Error: the --outputdir argument is required")
        sys.exit(1)
    s3url = args.source
    odir = args.outputdir

    sk = os.getenv("MINIO_SECRET_KEY")
    ak = os.getenv("MINIO_ACCESS_KEY")
    # end of parameters and environment variable section

    u, b, o = parse_s3_url(s3url)

    mc = Minio(u, ak, sk, secure=False)     # Create client with access and secret key.
    objects = mc.list_objects(b, prefix=o, recursive=True)       # get the object list from the path provided

    etlargs = [(obj, u, b, odir) for obj in objects]  # make tuple for etl call (pass S3 client here too?)

    pool = multiprocessing.Pool(processes=2)  # CAUTION: assume 2 Gb memory / thread approximately
    pool.map(etl, etlargs)
    pool.close()  # Close the pool
    pool.join()  # Wait for all processes to finish

if __name__ == '__main__':
    main()

