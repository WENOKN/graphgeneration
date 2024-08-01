import argparse
import io
import datetime
from pathlib import Path
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
import geopandas as gpd
from jinja2 import Template
from minio import Minio
from pyld import jsonld

import duckdb
import pyoxigraph

# need to conform to observation https://schema.org/Observation
#
def get_cell_id(longitude: float, latitude: float, level: int ) -> int:
    return CellId.from_lat_lng(LatLng.from_degrees(latitude, longitude)).parent(level).id()
def DATETIME_to_parts(d):
    year = int(d[0:4])
    month = int(d[4:6])
    day = int(d[6:8])
    hour = int(d[8:10])
    return year,month,day,hour
def to_ld(row):
    # r = str("test {}".format(row['Type']))
    # branch from here to RDF builders.

    # calculate our only new value to add Y -> lat, X -> long
    # print("{}  {}  {}".format(row['X'], row['Y'], s2cell13))
    s2cell18 = str(get_cell_id(row['X'], row['Y'], 18))
    s2cell13 = str(get_cell_id(row['X'], row['Y'], 13))
    # Type Description Address AreaSqm X Y Z SOURCE_ID UFOKN_ID FEATURE_ID GEOID
    # make dict
    # dwv 202404 -- it was determined that FEATURE_ID was actually the FEATURE_ID from the Catchement file/knowledgebase.
    # added a CATCHMENT_FEATURE_ID to better communicate this.
    year,month,day,hour = DATETIME_to_parts( row['DATETIME'])
    dt = datetime.datetime(year,month,day,hour)
    kwd = {"DATETIME": dt,
           "DATEREF": row['DATETIME'],
           "MAXDEPTH": row['max_depth'],

          "X": row['X'], "Y": row['Y'],
           #"Z": row['Z'],
       #    'SOURCE_ID': row['SOURCE_ID'],
           'UFOKN_ID': row['UFOKN_ID'],
          # 'FEATURE_ID': row['FEATURE_ID'],
           'GEOID': row['BLOCK'],
           'CellID13': s2cell13,
           'CellID18': s2cell18
           }
    # print("{} {} {}".format(kwd['X'], kwd['Y'], kwd['Z']))

    # read template and alter
    with open("flood_observations.json", "r") as file:
        template_str = file.read()

    template = Template(template_str)
    populated_json = template.render(kwd)

    nt = ""
    try:
        json_data = json.loads(populated_json)
        try:
            doc = jsonld.expand(json_data)
            nt = jsonld.normalize(doc, {'algorithm': 'URDNA2015'
                 ,'format': 'application/n-quads'
                                        }
                                  )
           # nt = jsonld.normalize(json_data, {'algorithm': 'URDNA2015', 'format': 'application/n-quads'})
            #nt = jsonld.normalize(populated_json, {'algorithm': 'URDNA2015'})
            #nt = jsonld.to_rdf(doc, options={format:'application/n-quads'})
        except Exception as e:
            print(e)
            # print(kwd)
            print("OFOKN_ID: {}  | Error converting to RDF".format(kwd['UFOKN_ID']))
    except Exception as e:
        print(e)
        # print(kwd)
        print("OFOKN_ID: {}  | ERROR loading json".format(kwd['UFOKN_ID']))

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



# NOTE URL TO  bgs enriched file https://oss.geocodes-aws.earthcube.org/valentine/wenokn/NAICS-SIC/oh_bgs_enriched.geojson
# 66 megs... so

def etl(etlargs):
    obj, u, b, odir, temp, datehour_part = etlargs
    session = boto3.Session(profile_name='ufokn')
    s3 = session.client('s3')
    tempdir = "graph_temp"
    # path = "s3://{}/{}/{}".format(u, b, obj.object_name)
    # print("Processing: {}".format(path))  # Each object is a dictionary containing details like object name, size, etc.

 #   s3: // backup.ufokn.forecasts.nwm - api / nwm - short_range / streamflow / 2023073015 _test.parquet
    buffer = io.BytesIO()
    impacts_path = f"impacts20/nwm-short_range/{datehour_part}_test.parquet"

    impacts_path = obj
    s3.download_fileobj("backup.ufokn.impacts.nwm-api",impacts_path ,
                        buffer)
    df = pd.read_parquet(buffer)
    if temp:
        df.to_parquet(f"{tempdir}/impacts_{datehour_part}.parquet".format(tempdir, datehour_part))
    print("Adding Geometry to data")
    df =  gpd.GeoDataFrame(
        df,
    #    geometry=gpd.points_from_xy(df.Y, df.X), crs="EPSG:4326")
        geometry = gpd.points_from_xy(df.X, df.Y), crs = "EPSG:4326") # geocorrds long lay
    #df=catchements_intersect(df)
    # dwv 202404 -- it was determined that FEATURE_ID was actually the FEATURE_ID from the Catchements source
    if temp:
        df.to_parquet(f"{tempdir}/impacts_fields_{datehour_part}.parquet")
    # print("loading catchements")
    # catchments_gdf = gpd.read_file("input/catchments.gpkg").to_crs("EPSG:4326")
    # print("spatial join on catchements")
    # df= df.sjoin(catchments_gdf, how="inner", predicate='within')
    # df.rename(columns={"feature_id", "Catchment_ID"})
    # print("spatial join on catchements complete")
    # Elevation Catch, run by group to ensure this isn't changing history
    #df['Z'] = df['Z'].fillna(0)
    df['DATETIME'] = datehour_part
    # build the RDF for each row
    results = df.apply(to_ld, axis=1)
    rdf = pd.DataFrame(results, columns=['rdf'])
    rdf.to_parquet(f"{odir}/impacts_{datehour_part}.parquet")
    del df
    del rdf
    s3.close()
    gc.collect()

def bulk_rdf(sdir=None, sfile=None, outfile=None):
    con = duckdb.connect()

    mime_type = "application/n-triples"
    store = pyoxigraph.Store(path="./store")

    store.clear()  # TODO:  make this a boolean flag in case you want to keep the store dir around

    if sdir is not None:
        print("Indexing directory: {}".format(sdir))
        con.execute("SELECT rdf FROM read_parquet('{}/*.parquet')".format(sdir))  # Replace with your query
    else:
        print("Indexing file: {}".format(sfile))
        con.execute("SELECT rdf FROM read_parquet('{}')".format(sfile))  # Replace with your query

    while True:
        row = con.fetchone()
        if row is None:
            break
        store.load(io.StringIO(row[0]), mime_type, base_iri=None, to_graph=None)

    output_mime_type = "application/n-quads"
    store.dump("{}".format(outfile), output_mime_type)
# TODO  add @id to the json-ld tempate
def main():
    # Parameters and environment variable section
    parser = argparse.ArgumentParser(description="Process some arguments.")
    parser.add_argument("--source", type=str, help="Source URL")
    parser.add_argument("--date", type=str, help="YYYYMMDDHH")
    parser.add_argument("--outputdir", type=str, help="Directory for output files")
    parser.add_argument("--temp", action=argparse.BooleanOptionalAction, help="write intermediate files to graph_temp directory")
    args = parser.parse_args()
    if args.source is None:
        print("Error: the --source argument is required")
       # print("using latest file from ufokn.impacts.nwm-api")
        sys.exit(1)
    if args.outputdir is None:
        print("Error: the --outputdir argument is required")
        sys.exit(1)
    if args.date is None:
        print("Latest Date is being used")
        datehour_part = datetime.datetime.now().strftime("%Y%m%d%H")
    else:
        datehour_part = args.date
    s3url = args.source
    #odir = args.outputdir
    odir = os.path.realpath(args.outputdir)

    sk = os.getenv("MINIO_SECRET_KEY")
    ak = os.getenv("MINIO_ACCESS_KEY")
    # end of parameters and environment variable section

    u, b, o = parse_s3_url(s3url)
    start_after_fn = f"{o}{datehour_part}_test.parquet"
    mc = Minio(u, ak, sk, secure=False)     # Create client with access and secret key.
   # objects = mc.list_objects(b, prefix=o, recursive=True, start_after=start_after_fn)       # get the object list from the path provided
   #  objects = mc.list_objects(b, prefix=o, recursive=False, start_after=start_after_fn)       # get the object list from the path provided
   #
   #  etlargs = [(obj, u, b, odir, args.temp, datehour_part) for obj in objects]  # make tuple for etl call (pass S3 client here too?)
   #
   #  pool = multiprocessing.Pool(processes=2)  # CAUTION: assume 2 Gb memory / thread approximately
   #  pool.map(etl, etlargs)
   #  pool.close()  # Close the pool
   #  pool.join()  # Wait for all processes to finish
    impacts_path = f"impacts20/nwm-short_range/{datehour_part}_test.parquet"

    etl([impacts_path, u, b, odir, args.temp, datehour_part])
    out=f"graph_temp/impacts_{datehour_part}.nq"
    file_parquet= f"{odir}/impacts_{datehour_part}.parquet"
    bulk_rdf(sdir=None, sfile=file_parquet, outfile=out)

if __name__ == '__main__':
    main()


