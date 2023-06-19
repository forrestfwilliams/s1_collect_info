import json
import requests
from argparse import ArgumentParser
from datetime import datetime
from lxml import etree
from pathlib import Path
from urllib.request import urlopen

import asf_search as asf
from bs4 import BeautifulSoup
import geopandas as gpd
from shapely import LinearRing
from shapely.geometry import shape, Polygon
import pandas as pd


def scrape_esa_website_for_download_urls():
    url = 'https://sentinel.esa.int/web/sentinel/missions/sentinel-1/observation-scenario/acquisition-segments'
    page = urlopen(url)
    html = page.read().decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')
    div = soup.find_all('div', class_='sentinel-1a')[0]
    ul = div.find('ul')
    hrefs = [a['href'] for a in ul.find_all('a')]
    download_urls = [f'https://sentinel.esa.int{href}' for href in hrefs]
    return download_urls


def download_kml(url, out_name='collection.kml'):
    response = requests.get(url)
    if response.status_code == 200:
        with open(out_name, 'wb') as file:
            file.write(response.content)
        print('File downloaded successfully.')
    else:
        print('Failed to download the file.')

    return out_name


def parse_placemark(placemark: etree.Element):
    prefix = './/{http://www.opengis.net/kml/2.2}'

    begin_date = placemark.find(f'{prefix}begin').text
    begin_date = datetime.fromisoformat(begin_date)

    end_date = placemark.find(f'{prefix}end').text
    end_date = datetime.fromisoformat(end_date)

    data = placemark.find(f'{prefix}ExtendedData')
    mode = data.find(f"{prefix}Data[@name='Mode']").find(f'{prefix}value').text
    orbit_absolute = int(data.find(f"{prefix}Data[@name='OrbitAbsolute']").find(f'{prefix}value').text)
    orbit_relative = int(data.find(f"{prefix}Data[@name='OrbitRelative']").find(f'{prefix}value').text)

    footprint = placemark.find(f'{prefix}LinearRing').find(f'{prefix}coordinates').text
    x_coords = [float(point.split(',')[0]) for point in footprint.split(' ')]
    y_coords = [float(point.split(',')[1]) for point in footprint.split(' ')]
    footprint = LinearRing(zip(x_coords, y_coords))

    return (begin_date, end_date, mode, orbit_absolute, orbit_relative, footprint)


def parse_kml(kml_path: Path):
    placemark_pattern = './/{http://www.opengis.net/kml/2.2}Placemark'
    tree = etree.parse(kml_path).getroot()
    placemarks = [parse_placemark(elem) for elem in tree.findall(placemark_pattern)]
    columns = ['begin_date', 'end_date', 'mode', 'orbit_absolute', 'orbit_relative', 'geometry']
    gdf = gpd.GeoDataFrame(data=placemarks, columns=columns, geometry='geometry', crs='EPSG:4326')
    return gdf


def prep_collection_plan(out_path='collection.geojson'):
    urls = scrape_esa_website_for_download_urls()

    gdfs = []
    for url in urls:
        name = Path(url).name
        collection_gdf_path = Path(f'{name}.geojson')

        if collection_gdf_path.exists():
            print('Collection already prepared.')
            gdf = gpd.read_file(collection_gdf_path)
        else:
            file_path = download_kml(url)
            gdf = parse_kml(file_path)
            gdf.to_file(collection_gdf_path)
        gdfs.append(gdf)

    full_gdf = pd.concat(gdfs).drop_duplicates()
    full_gdf = full_gdf.loc[full_gdf['begin_date'] >= datetime.now()].copy()
    full_gdf = full_gdf.sort_values('begin_date', ascending=True).reset_index(drop=True)

    full_gdf.to_file(out_path)
    return collection_gdf_path


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


def get_next_collect(granule):
    prep_collection_plan()
    gdf = gpd.read_file('collection.geojson')
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
    print(event)
    granule = event['pathParameters']['granule']
    message = get_next_collect(granule)

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
    message = get_next_collect(args.granule)
    print(message)


if __name__ == '__main__':
    main()
