import json
import os
from argparse import ArgumentParser

import asf_search as asf
import boto3
import geopandas as gpd
from shapely.geometry import shape, Polygon, Point
from typing import Iterable, Optional, Tuple, Union


S3 = boto3.client('s3')
DATASET_BUCKET_NAME = os.environ.get('DatasetBucketName')

if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ:
    COLLECTION_DATASET = gpd.read_file(f's3://{DATASET_BUCKET_NAME}/collection.geojson')


def get_granule_info(granule: str) -> Iterable:
    result = asf.granule_search(granule)[0]

    footprint = shape(result.geometry)
    mode = result.properties['beamModeType']
    orbit_relative = result.properties['pathNumber']
    return footprint, mode, orbit_relative


def find_valid_insar_collects(collections: gpd.GeoDataFrame, mode: str, orbit_relative: int) -> gpd.GeoDataFrame:
    filtered = collections.loc[(collections['orbit_relative'] == orbit_relative) & (collections['mode'] == mode)]
    return filtered


def find_valid_collect(
    gdf: gpd.GeoDataFrame, footprint: Union[Polygon, Point], mode=None
) -> Tuple[bool, gpd.GeoDataFrame]:
    gdf = gdf.loc[gdf['geometry'].intersects(footprint)].copy()

    if gdf.shape[0] > 0:
        collect_scheduled = True
        gdf = gdf.sort_values('begin_date', ascending=True).reset_index(drop=True)
        next_collect = gdf['begin_date'][0].date()
    else:
        collect_scheduled = False
        next_collect = None
    return collect_scheduled, next_collect


def get_next_collect(point: Point, collection_dataset: gpd.GeoDataFrame, mode: Optional[str] = None) -> str:
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


def get_next_interferometric_collect(granule: str, collection_dataset: gpd.GeoDataFrame) -> str:
    footprint, mode, orbit_relative = get_granule_info(granule)
    valid_insar_collects = find_valid_insar_collects(collection_dataset, mode, orbit_relative)
    collect_scheduled, next_collect = find_valid_collect(valid_insar_collects, footprint)
    if collect_scheduled:
        message = f'Next interferometrically valid collect is {next_collect}'
    else:
        max_date = collection_dataset['end_date'].max().date()
        message = f'No interferometrically valid collect is scheduled on or before {max_date}'

    return message


def lambda_handler(event: dict, context: dict) -> dict:
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
    parser = ArgumentParser()
    parser.add_argument('interface', choices=['scene', 'location'], default='scene')
    parser.add_argument('--scene', default=None, required=False)
    parser.add_argument('--lon', default=None, required=False, type=float)
    parser.add_argument('--lat', default=None, required=False, type=float)
    parser.add_argument('--mode', default=None, required=False)
    args = parser.parse_args()
    collection_dataset = gpd.read_file('collection.geojson')
    if args.interface == 'scene':
        message = get_next_interferometric_collect(args.scene, collection_dataset)
    elif args.interface == 'location':
        point = Point(args.lon, args.lat)
        message = get_next_collect(point, collection_dataset, args.mode)
    print(message)


if __name__ == '__main__':
    main()
