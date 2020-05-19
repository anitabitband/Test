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
    'IMG': {
        'filename' : 'IMG.json',
        'external_name' : 'VLASS1.1.ql.T01t01.J000232-383000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits',
        'file_count' : 2
    },
    'VLBA_EB': {
        'filename' : 'VLBA_EB.json',
        'external_name' : '',
        'file_count' : 17
    },

}

def get_locations_file(key: str):
    ''' return the location report file specified by key '''
    report_spec = LOCATION_REPORTS[key]
    filename = report_spec['filename']
    test_data_dir = os.path.join(os.curdir, 'data')
    return os.path.join(test_data_dir, filename)

def get_locations_report(key: str):
    ''' return the location report specified by key '''
    report_file = get_locations_file(key)
    with open(report_file, 'r') as content:
        locations_report = json.loads(content.read())
    return locations_report

def write_locations_file(destination: Path, locations_report: LocationsReport):
    ''' write locations report to a file '''
    with open(destination, 'w') as to_write:
        to_dump = {'files': locations_report['files']}
        json.dump(to_dump, to_write, indent=4)
    return destination

def get_mini_exec_block():
    ''' return a location report with large files excised
    '''
    locations_in = get_locations_report('VLA_SMALL')
    locations_out = locations_in.copy()
    locations_out['files'] = \
        [file for file in locations_in['files'] if file['size'] <= 100000]
    return locations_out

def get_mini_locations_file(destination: Path):
    ''' return a location report file with large files excised
    '''
    locations_report = get_mini_exec_block()
    with open(destination, 'w') as to_write:
        to_dump = {'files': locations_report['files']}
        json.dump(to_dump, to_write, indent=4)
    return destination
