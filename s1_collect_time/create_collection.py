import os
import requests
import tempfile
from datetime import datetime
from lxml import etree
from pathlib import Path
from urllib.request import urlopen

import boto3
from bs4 import BeautifulSoup
import geopandas as gpd
from shapely import LinearRing, Polygon
import pandas as pd

S3 = boto3.client('s3')
DATASET_BUCKET_NAME = os.environ.get('DatasetBucketName')


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


def download_kml(url, out_path='collection.kml'):
    response = requests.get(url)
    if response.status_code == 200:
        with open(out_path, 'wb') as file:
            file.write(response.content)
        print('File downloaded successfully.')
    else:
        print('Failed to download the file.')

    return out_path


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
    footprint = Polygon(LinearRing(zip(x_coords, y_coords)))

    return (begin_date, end_date, mode, orbit_absolute, orbit_relative, footprint)


def parse_kml(kml_path: Path):
    placemark_pattern = './/{http://www.opengis.net/kml/2.2}Placemark'
    tree = etree.parse(kml_path).getroot()
    placemarks = [parse_placemark(elem) for elem in tree.findall(placemark_pattern)]
    columns = ['begin_date', 'end_date', 'mode', 'orbit_absolute', 'orbit_relative', 'geometry']
    gdf = gpd.GeoDataFrame(data=placemarks, columns=columns, geometry='geometry', crs='EPSG:4326')
    return gdf


def create_collection_plan(out_name='collection.geojson', dir=Path('.')):
    urls = scrape_esa_website_for_download_urls()

    gdfs = []
    for url in urls:
        name = Path(url).name
        collection_gdf_path = dir / f'{name}.geojson'

        if collection_gdf_path.exists():
            print('Collection already prepared.')
            gdf = gpd.read_file(collection_gdf_path)
        else:
            file_path = download_kml(url, dir / 'collection.kml')
            gdf = parse_kml(file_path)
            gdf.to_file(collection_gdf_path)
        gdfs.append(gdf)

    full_gdf = pd.concat(gdfs).drop_duplicates()
    full_gdf = full_gdf.loc[full_gdf['begin_date'] >= datetime.now()].copy()
    full_gdf = full_gdf.sort_values('begin_date', ascending=True).reset_index(drop=True)

    out_path = dir / out_name
    full_gdf.to_file(out_path)
    return out_path


def lambda_handler(event, context):
    with tempfile.TemporaryDirectory() as tmpdirname:
        out_path = create_collection_plan(dir=Path(tmpdirname))
        S3.upload_file(str(out_path), DATASET_BUCKET_NAME, out_path.name)

    print('Success!')
    return None


def main():
    return None


if __name__ == '__main__':
    create_collection_plan()
