import dlt
import json

from functions import extract_file, change_array_to_linestring, change_array_to_polygon

segments_file_name = "data/EMAP_Raw_PipeSegments.geojson"
countries_file_name = "countries.geojson"
zipped_file_path = "EMAP_Raw.zip"
segments_URL = 'https://zenodo.org/records/3985298/files/EMAP_Raw.zip'
countries_url = 'https://raw.githubusercontent.com/leakyMirror/map-of-europe/refs/heads/master/GeoJSON/europe.geojson'
extract_path = 'extracted_files/'

# Download and extract a geojson file
segments = extract_file(segments_URL, zipped_file_path, segments_file_name, extract_path)
countries = extract_file(countries_url, countries_file_name, None, None)

SEGMENTS_EXTRACTED_FILE_PATH = extract_path + segments_file_name
COUNTRIES_EXTRACTED_FILE_PATH = countries_file_name

# Custom extractor to read JSON and start from the 'features' key
#dlt.resource(max_table_nesting=0) #to disable normalization of all columns
@dlt.resource()
def extract_features_from_geojson(file_path: str):
    with open(file_path, 'r') as f:
        # Load the JSON data
        data = json.load(f)
        
        # Access the 'features' key, which is a list of features
        features = data.get('features', [])
        
        # Yield each feature
        for feature in features:
            yield feature

pipeline = dlt.pipeline(
    pipeline_name="emap",
    destination='postgres',
    dataset_name="pipe_segments",
    import_schema_path="schema/",
    full_refresh=True,
    dev_mode=True
)

# Use the custom extractor to process the file starting from 'features'
load_info = pipeline.run((extract_features_from_geojson(SEGMENTS_EXTRACTED_FILE_PATH)).add_map(change_array_to_linestring), table_name="pipe_segments")
load_info = pipeline.run((extract_features_from_geojson(COUNTRIES_EXTRACTED_FILE_PATH)).add_map(change_array_to_polygon), table_name="countries")
print(load_info)
