# -*- coding: utf-8 -*-

# Implementations of assorted product fetchers

import copy
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from yoink.errors import NGASServiceErrorException, terminal_exception
from yoink.file_retrievers import NGASFileRetriever


class BaseProductFetcher:
    """ This is a base class for fetchers. """

    def __init__(self, args, settings, servers_report):
        self.log = logging.getLogger(self.__class__.__name__)
        self.args = args
        self.output_dir = args.output_dir
        self.force_overwrite = args.force
        self.dry_run = args.dry_run
        self.servers_report = servers_report
        self.settings = settings
        self.ngas_retriever = NGASFileRetriever(args)
        self.retrieved = []

    def retrieve_files(self, server, retrieve_method, file_specs):
        retriever = NGASFileRetriever(self.args)
        num_files = len(file_specs)
        count = 0

        self.log.info(f'>>> Got {num_files} files to retrieve from {server} by {retrieve_method}')
        try:
            for file_spec in file_specs:
                count += 1
                self.log.info(f">>> retrieving {file_spec['relative_path']} "
                              f"({file_spec['size']} bytes, no. {count} of {num_files})....")
                self.retrieved.append(retriever.retrieve(server, retrieve_method, file_spec))
            self.log.info(f'>>> {len(self.retrieved)} files retrieved from {server} on this pass.')
            return num_files
        except NGASServiceErrorException as n_exc:
            raise n_exc


class SerialProductFetcher(BaseProductFetcher):
    """ Pull the files out, one right after another; don't try to be clever about it.
    """

    def __init__(self, args, settings, servers_report):
        super().__init__(args, settings, servers_report)

    def run(self):
        self.log.debug('writing to {}'.format(self.output_dir))
        self.log.debug('dry run: {}'.format(self.dry_run))
        self.log.debug(f'force overwrite: {self.force_overwrite}')
        for server in self.servers_report:
            self.retrieve_files(server,
                                self.servers_report[server]['retrieve_method'],
                                self.servers_report[server]['files'])


class ParallelProductFetcher(BaseProductFetcher):
    """ Pull the files out in parallel; try to be clever about it.
    """

    def __init__(self, args, settings, servers_report):
        super().__init__(args, settings, servers_report)
        self.num_files_expected = self._count_files_expected()
        self.bucketized_files = self._bucketize_files()

    def _count_files_expected(self):
        count = 0;
        for server in self.servers_report:
            count += len(self.servers_report[server]['files'])
        return count

    def _bucketize_files(self):
        """ Takes the servers report and splits it up into a list of buckets, each
        of which a single thread will handle. There will be X * Y buckets, where X
        is the number of servers and Y is the 'threads per host', and the files for
        a given server will be distributed among the buckets for that server.

        Basically what we are doing here is splitting up the work among the threads
        we'll be creating and creating a list of work for each thread to do.
        """

        result = list()
        for server in self.servers_report:
            # Setup the 'buckets', one per server.
            bucket = {'server': server,
                      'retrieve_method': self.servers_report[server]['retrieve_method'],
                      'files': list()}
            buckets = [copy.deepcopy(bucket) for x in
                       range(int(self.settings['threads_per_host']))]
            # Spread the files for a given server around its buckets.
            i = 0
            for f in self.servers_report[server]['files']:
                list_number = i % int(self.settings['threads_per_host'])
                buckets[list_number]['files'].append(f)
                i += 1
            # Trim out every bucket with no files, add the rest to the result.
            result.extend([bucket for bucket in
                           buckets if len(bucket['files']) > 0])
        return result

    def fetch_bucket(self, bucket):
        try:
            self.retrieve_files(bucket['server'],
                                bucket['retrieve_method'],
                                bucket['files'])
        except NGASServiceErrorException as exc:
            raise exc

    def run(self):
        with ThreadPoolExecutor() as executor:
            results = executor.map(self.fetch_bucket, self.bucketized_files)
            num_files_retrieved = 0
            try:
                for future in as_completed(results):
                    # Doesn't actually return anything, but gooses any exceptions.
                    # This could be made cooled somehow, like, have it return the
                    # number of files fetched.
                    num_files_retrieved += future.result()
                    if (num_files_retrieved != self.num_files_expected):
                        # TODO: throw exception
                        self.log.error(f'{self.num_files_expected} files expected, '
                                       f'but only {num_files_retrieved} retrieved')
                        return num_files_retrieved
                return self.retrieved
            except NGASServiceErrorException as n_exc:
                raise n_exc
            except AttributeError as a_err:
                # TODO: is this really spurious, thrown only when there are no more files to retrieve?
                self.log.error(f'>>> {a_err}')
                return self.retrieved
