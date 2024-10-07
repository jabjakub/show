import requests
import zipfile
import logging
from shapely.geometry import LineString, MultiPolygon, Polygon

logging.basicConfig(
    level=logging.INFO,  # Set the minimum log level
    )

logger = logger = logging.getLogger('logger')

def download_and_save_zip(url, save_path):
    # Send HTTP request to download the file
    response = requests.get(url, stream=True)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Save the zip file to the specified path
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=128):
                file.write(chunk)
        return logger.info(f"Downloaded file saved as {save_path}")
    else:
        return logger.error(f"Failed to download the file. Status code: {response.status_code}")

def extract_file(url, save_path, file_name, extract_to):
    download_and_save_zip(url, save_path)
    if extract_to is not None:
        with zipfile.ZipFile(save_path, 'r') as zip_ref:
            # Extract only the specific file
            zip_ref.extract(file_name, extract_to)
            return logger.info(f"Extracted {file_name} to {extract_to}")
    
def change_array_to_linestring(df):
    df['geometry']['coordinates'] = str(LineString(df['geometry']['coordinates']))
    return df

def change_array_to_polygon(df):
    if df['geometry']['type'] == 'MultiPolygon':
        polygons = [Polygon(polygon[0]) for polygon in df['geometry']['coordinates']]
        polygons = MultiPolygon(polygons)
    else:
        polygons = Polygon(df['geometry']['coordinates'][0])

    df['geometry']['coordinates'] = str(polygons)
    return df