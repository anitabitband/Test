# -*- coding: utf-8 -*-

# Implementations of assorted product fetchers

import logging
from threading import Thread

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

    def run(self):
        pass

    def verify_files(self):
        pass

    def verify_file(self):
        pass


class SerialProductFetcher(BaseProductFetcher):
    """ Pull the files out, one right after another, don't try to be
    clever about it. """

    def __init__(self, args, settings, servers_report):
        super().__init__(args, settings, servers_report)

    def run(self):
        self.log.debug('writing to {}'.format(self.output_dir))
        self.log.debug('dry run: {}'.format(self.dry_run))
        for server in self.servers_report:
            retrieve_method = self.servers_report[server]['retrieve_method']
            for f in self.servers_report[server]['files']:
                self.ngas_retriever.retrieve(server, retrieve_method, f)


def retrieve_files(args, server, retrieve_method, file_specs):
    retriever = NGASFileRetriever(args)
    for file_spec in file_specs:
        retriever.retrieve(server, retrieve_method, file_spec)


class ParallelProductFetcher(BaseProductFetcher):
    """ Pull the files out in parallel, try to be clever about it. Likely
    fail in the attempt, but do try to be clever. """

    # TODO: IMO this poorly handles the case where a threaded request fails, what
    #   should happen is the low level routine should throw an exception if there
    #   is an error that any product fetcher should handle.

    def __init__(self, args, settings, servers_report):
        super().__init__(args, settings, servers_report)
        self.bucketized_files = self._bucketize_files()

    def _bucketize_files(self):
        """ bucketize files takes the list of files to be fetched and breaks
        them up for each thread: the end result of this should be a dictionary
        with one entry per server in the locations, and the value of that key
        should be a list of lists: one list per 'threadsPerHost', and each of
        those lists are files to be retrieved.

        I don't think I explained that well. Lets say you have asked for a huge
        product, a VLASS execution block with 27k files, spread across three
        different NGAS servers.

        The result of bucketization is a dictionary with three keys, one per
        each NGAS host. The value of the key is list with one element per the
        number of threads per host (currently configured at 4). That per-server
        and per-thread value is a list of files that thread will fetch.
        """
        result = dict()
        # One dict entry per server.
        for server in self.servers_report:
            # Each entry has a list of 'threads_per_host' elements
            result[server] = list()
            for i in range(int(self.settings['threads_per_host'])):
                result[server].append(list())
            # Assign files to threads, tried this with a comprehension but it
            # was ... fairly incomprehensible.
            i = 0
            for f in self.servers_report[server]['files']:
                list_number = i % int(self.settings['threads_per_host'])
                result[server][list_number].append(f)
                i += 1
        return result

    def run(self):
        threads = list()
        for server in self.bucketized_files:
            retrieve_method = self.servers_report[server]['retrieve_method']
            self.log.debug('building thread, server: {}, method: {}'
                           .format(server, retrieve_method))
            for file_specs in self.bucketized_files[server]:
                thread = Thread(target=retrieve_files,
                                args=(self.args, server, retrieve_method,
                                      file_specs,))
                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()
