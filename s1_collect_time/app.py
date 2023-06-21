import json
import tempfile
from argparse import ArgumentParser
from pathlib import Path

import asf_search as asf
import geopandas as gpd
from shapely.geometry import shape, Polygon


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
    # gpd.GeoDataFrame({'id': [1], 'geometry': [footprint]}, crs='EPSG:4326').to_file('scene.geojson')
    collect_scheduled, next_collect = find_valid_collect(gdf, footprint, mode, orbit_relative)
    if collect_scheduled:
        message = f'Next interferometrically valid collect is {next_collect}'
    else:
        message = f'No interferometrically valid collect is scheduled on or before {max_date}'

    return message


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)

    #     raise e
    import os
    import boto3

    s3 = boto3.client('s3')
    dataset_bucket_name = os.environ.get('DatasetBucketName')
    granule = event['pathParameters']['granule']
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname)
        collection_name = 'collection.geojson'
        tmp_collection = str(tmpdir / collection_name)
        print(
            dataset_bucket_name,
            type(dataset_bucket_name),
            tmp_collection,
            type(tmp_collection),
            collection_name,
            type(collection_name),
        )
        s3.download_file(dataset_bucket_name, collection_name, tmp_collection)
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
