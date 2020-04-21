# -*- coding: utf-8 -*-

# Implementations of assorted product fetchers

import copy
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from yoink.file_retrievers import NGASFileRetriever


class BaseProductFetcher:
    """ This is a base class for fetchers. """

    # TODO: think it over, and either make this more useful or nuke it,
    #   because it doesn't seem to do enough to justify itself.

    def __init__(self, args, settings, servers_report):
        self.log = logging.getLogger(self.__class__.__name__)
        self.args = args
        self.output_dir = args.output_dir
        self.dry_run = args.dry_run
        self.servers_report = servers_report
        self.settings = settings
        self.ngas_retriever = NGASFileRetriever(args)

    def retrieve_files(self, server, retrieve_method, file_specs):
        retriever = NGASFileRetriever(self.args)
        for file_spec in file_specs:
            retriever.retrieve(server, retrieve_method, file_spec)


class SerialProductFetcher(BaseProductFetcher):
    """ Pull the files out, one right after another, don't try to be
    clever about it. """

    def __init__(self, args, settings, servers_report):
        super().__init__(args, settings, servers_report)

    def run(self):
        self.log.debug('writing to {}'.format(self.output_dir))
        self.log.debug('dry run: {}'.format(self.dry_run))
        for server in self.servers_report:
            self.retrieve_files(server,
                                self.servers_report[server]['retrieve_method'],
                                self.servers_report[server]['files'])


class ParallelProductFetcher(BaseProductFetcher):
    """ Pull the files out in parallel, try to be clever about it. Likely
    fail in the attempt, but do try to be clever. """

    def __init__(self, args, settings, servers_report):
        super().__init__(args, settings, servers_report)
        self.bucketized_files = self._bucketize_files()

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
        self.retrieve_files(bucket['server'],
                            bucket['retrieve_method'],
                            bucket['files'])

    def run(self):
        with ThreadPoolExecutor() as executor:
            results = executor.map(self.fetch_bucket, self.bucketized_files)
            for future in as_completed(results):
                # Doesn't actually return anything, but gooses any exceptions.
                # This could be made cooled somehow, like, have it return the
                # number of files fetched.
                result = future.result()
