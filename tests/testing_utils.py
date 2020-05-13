import json
import os
from pathlib import Path

from yoink.utilities import LocationsReport

LOCATION_REPORTS = {
    'VLA_SMALL': {
    'filename' : 'VLA_SMALL_EB.json',
    'external_name' : 'TSKY_20min_B2319_18ms_001.58955.86469591435',
    'file_count' : 44
    },
}

def get_locations_file(key: str):
    report_spec = LOCATION_REPORTS[key]
    filename = report_spec['filename']
    test_data_dir = os.path.join(os.curdir, 'data')
    return os.path.join(test_data_dir, filename)

def get_locations_report(key: str):
    report_file = get_locations_file(key)
    with open(report_file, 'r') as content:
        locations_report = json.loads(content.read())
    return locations_report

def write_locations_file(destination: Path, locations_report: LocationsReport):
    with open(destination, 'w') as f:
        to_dump = {'files': locations_report['files']}
        json.dump(to_dump, f, indent=4)
    return destination

def get_mini_exec_block():
    locations_in = get_locations_report('VLA_SMALL')
    files_to_keep = [file for file in locations_in['files'] if file['size'] <= 100000]
    locations_out = locations_in.copy()
    locations_out['files'] = [file for file in files_to_keep]
    return locations_out

def get_mini_locations_file(destination: Path):
    locations_report = get_mini_exec_block()
    with open(destination, 'w') as f:
        to_dump = {'files': locations_report['files']}
        json.dump(to_dump, f, indent=4)
    return destination
