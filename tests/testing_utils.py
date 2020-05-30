""" Various conveniences for use and re-use in test cases """

import json
import os
from pathlib import Path

from yoink.locations_report import LocationsReport
from yoink.utilities import get_arg_parser

LOCATION_REPORTS = {
    'VLA_SMALL_EB': {
        'filename' : 'VLA_SMALL_EB.json',
        'external_name' : 'TSKY_20min_B2319_18ms_001.58955.86469591435',
        'file_count' : 44,
        'server_count' : 2
    },
    'VLA_LARGE_EB': {
        'filename' : 'VLA_LARGE_EB.json',
        'external_name' : '17B-197.sb34812522.eb35115211.58168.58572621528',
        'file_count' : 46,
        'server_count' : 2
    },
    'VLA_BAD_SERVER': {
        'filename' : 'VLA_BAD_SERVER.json',
        'external_name' : 'TSKY_20min_B2319_18ms_001.58955.86469591435',
        'file_count' : 1,
        'server_count' : 1
    },
    'IMG': {
        'filename' : 'IMG.json',
        'external_name' :
            'VLASS1.1.ql.T01t01.J000232-383000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits',
        'file_count' : 2,
        'server_count' : 2
    },
    'VLBA_EB': {
        'filename' : 'VLBA_EB.json',
        'external_name' : '',
        'file_count' : 16,
        'server_count' : 1
    },
    'CALIBRATION': {
        'filename' : 'CALIBRATION.json',
        'external_name' : '18B-265_2019_12_10_T00_00_59.203.tar',
        'file_count' : 1,
        'server_count' : 1
    },

}

DATA_DIR = './data/'

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
    locations_in = get_locations_report('VLA_SMALL_EB')
    locations_out = locations_in.copy()
    locations_out['files'] = \
        [file for file in locations_in['files'] if file['size'] <= 100000]
    return locations_out

def get_mini_locations_file(destination):
    ''' return a location report file with large files excised
    '''
    locations_report = get_mini_exec_block()
    with open(destination, 'w') as to_write:
        to_dump = {'files': locations_report['files']}
        json.dump(to_dump, to_write, indent=4)
    return destination

def get_filenames_for_locator(product_locator: str,
                              settings: dict,
                              sdm_only: bool):
    '''
    For a given product locators, return names of all the files
    in its locations report's files report
    :param product_locator:
    :param settings:
    :param sdm_only:
    :return:
    '''
    args = ['--product-locator', product_locator,
            '--profile', 'local', '--output-dir', None]
    if sdm_only:
        args.append('--sdm-only')
    namespace = get_arg_parser().parse_args(args)
    locations_report = LocationsReport(namespace, settings)

    return [file['relative_path'] for file in
            locations_report.files_report['files']]

def find_yoink_log_file(target_dir: Path):
    ''' yoink command line was executed; find the log
    '''
    for root, dirnames, filenames in os.walk(target_dir):
        for filename in filenames:
            if filename.startswith('Yoink_') and filename.endswith('.log'):
                return os.path.join(root, filename)
    return None