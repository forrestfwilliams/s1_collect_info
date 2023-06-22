import json
import os
from argparse import ArgumentParser

import asf_search as asf
import boto3
import geopandas as gpd
from shapely.geometry import shape, Polygon, Point
from typing import Union


S3 = boto3.client('s3')
DATASET_BUCKET_NAME = os.environ.get('DatasetBucketName')
COLLECTION_DATASET = gpd.read_file(f's3://{DATASET_BUCKET_NAME}/collection.geojson')


def get_granule_info(granule: str):
    result = asf.granule_search(granule)[0]

    footprint = shape(result.geometry)
    mode = result.properties['beamModeType']
    orbit_relative = result.properties['pathNumber']
    return footprint, mode, orbit_relative


def find_valid_insar_collects(collections: gpd.GeoDataFrame, mode: str, orbit_relative: int):
    filtered = collections.loc[(collections['orbit_relative'] == orbit_relative) & (collections['mode'] == mode)]
    return filtered


def find_valid_collect(gdf: gpd.GeoDataFrame, footprint: Union[Polygon, Point], mode=None):
    gdf = gdf.loc[gdf['geometry'].intersects(footprint)].copy()

    if gdf.shape[0] > 0:
        collect_scheduled = True
        gdf = gdf.sort_values('begin_date', ascending=True).reset_index(drop=True)
        next_collect = gdf['begin_date'][0].date()
    else:
        collect_scheduled = False
        next_collect = None
    return collect_scheduled, next_collect


def get_next_collect(point, collection_dataset, mode=None):
    mode_msg = ' '
    if mode:
        collection_dataset = collection_dataset.loc[collection_dataset['mode'] == mode].copy()
        mode_msg = f' {mode} '

    collect_scheduled, next_collect = find_valid_collect(collection_dataset, point)
    if collect_scheduled:
        message = f'Next{mode_msg}collect is {next_collect}'
    else:
        max_date = collection_dataset['end_date'].max().date()
        message = f'No{mode_msg}collect is scheduled on or before {max_date}'

    return message


def get_next_interferometric_collect(granule, collection_dataset):
    footprint, mode, orbit_relative = get_granule_info(granule)
    valid_insar_collects = find_valid_insar_collects(collection_dataset, mode, orbit_relative)
    collect_scheduled, next_collect = find_valid_collect(valid_insar_collects, footprint)
    if collect_scheduled:
        message = f'Next interferometrically valid collect is {next_collect}'
    else:
        max_date = collection_dataset['end_date'].max().date()
        message = f'No interferometrically valid collect is scheduled on or before {max_date}'

    return message


def lambda_handler(event, context):
    url_path = event['path']
    query_params = event['queryStringParameters']
    print(url_path)
    print(query_params)
    if url_path == '/s1-collect-info/scene':
        granule = query_params['scene']
        message = get_next_interferometric_collect(granule, COLLECTION_DATASET)
    elif url_path == '/s1-collect-info/location':
        if 'mode' in query_params:
            mode = query_params['mode']
        else:
            mode = None
        point = Point([query_params['lon'], query_params['lat']])
        message = get_next_collect(point, COLLECTION_DATASET, mode)
    else:
        message = f'{url_path} is not a valid path'

    return {
        'statusCode': 200,
        'body': json.dumps(
            {
                'message': message,
            }
        ),
    }


def main():
    # Test Case 1: S1A_IW_SLC__1SDV_20180405T023745_20180405T023812_021326_024B31_FBCC-SLC
    # Test Case 2: S1B_IW_SLC__1SDV_20180628T151540_20180628T151607_011575_015476_4673-SLC
    parser = ArgumentParser()
    parser.add_argument('granule')
    args = parser.parse_args()
    message = get_next_interferometric_collect(args.granule, COLLECTION_DATASET)
    print(message)


if __name__ == '__main__':
    main()
