import json
import os
import tempfile
from argparse import ArgumentParser
from pathlib import Path

import asf_search as asf
import boto3
import geopandas as gpd
from shapely.geometry import shape, Polygon

S3 = boto3.client('s3')
DATASET_BUCKET_NAME = os.environ.get('DatasetBucketName')


def get_granule_info(granule: str):
    result = asf.granule_search(granule)[0]

    footprint = shape(result.geometry)
    mode = result.properties['beamModeType']
    orbit_relative = result.properties['pathNumber']
    return footprint, mode, orbit_relative


def find_valid_collect(gdf: gpd.GeoDataFrame, footprint: Polygon, mode: str, orbit_relative: int):
    filtered = gdf.loc[(gdf['orbit_relative'] == orbit_relative) & (gdf['mode'] == mode)]
    filtered = filtered.loc[filtered['geometry'].intersects(footprint)].copy()

    if filtered.shape[0] > 0:
        collect_scheduled = True
        filtered = filtered.sort_values('begin_date', ascending=True).reset_index(drop=True)
        next_collect = filtered['begin_date'][0].date()
    else:
        collect_scheduled = False
        next_collect = None
    return collect_scheduled, next_collect


def get_next_collect(granule, dir=Path('.')):
    gdf = gpd.read_file(dir / 'collection.geojson')
    max_date = gdf['end_date'].max().date()
    footprint, mode, orbit_relative = get_granule_info(granule)
    collect_scheduled, next_collect = find_valid_collect(gdf, footprint, mode, orbit_relative)
    if collect_scheduled:
        message = f'Next interferometrically valid collect is {next_collect}'
    else:
        message = f'No interferometrically valid collect is scheduled on or before {max_date}'

    return message


def lambda_handler(event, context):
    granule = event['pathParameters']['granule']
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname)
        collection_name = 'collection.geojson'
        tmp_collection = str(tmpdir / collection_name)
        S3.download_file(DATASET_BUCKET_NAME, collection_name, tmp_collection)
        message = get_next_collect(granule, dir=tmpdir)

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
    message = get_next_collect(args.granule, dir=Path('.'))
    print(message)


if __name__ == '__main__':
    main()
