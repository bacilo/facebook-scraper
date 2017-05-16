# -*- coding: utf-8 -*-
"""Producer/Consummer pool

Multithreading processing of responses from batch requests and issues more
requests accordingly
"""
import queue
import threading
import time
import logging

from fb_scraper import FSRequestBatch

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s - %(funcName)s): %(message)s',)


class Manager(object):
    """
    This class is responsible for managing the producer/consummer threads
    """
    _QUEUE_BUF_SIZE = 50000

    def __init__(self, credentials):
        """
        Initializes the manager object and starts shared queues and
        threading objects
        """
        self.req_queue = queue.Queue(self._QUEUE_BUF_SIZE)
        self.resp_queue = queue.Queue(self._QUEUE_BUF_SIZE)
        self.credentials = credentials
        self.proc_data = ProcessData(parent=self)
        self.req_issuer = [RequestIssuer(parent=self) for i in range(5)]
        self.jobs = dict()
        self._isscraping = True

    def add_job(self, job):
        """
        Adds a Job to the manager
        """
        self.jobs[job.job_id] = job

    # def scrape_post(self, post_id):
    #     """ Initiaties the scraping of a single post """
    #     job = PostJob(post_id, self.add_request)
    #     self.jobs[job.job_id] = job
    #     self.req_queue.put(self.graph.create_post_request(post_id,
    #                                                       job.job_id))

    def start(self):
        """Starts the threads"""
        for ri in self.req_issuer:
            ri.start()
            time.sleep(2)
        self.proc_data.start()
        time.sleep(2)

    def stop(self):
        """Stops the threads"""
        self._isscraping = False

    def is_scraping(self):
        """Determins if scraping has ended"""
        return self._isscraping


class ProcessData(threading.Thread):
    """
    This class defines a thread that processes data received from the FB API
    """
    def __init__(self, parent):
        """
        Initializes the thread object for processing received data.
        """
        super(ProcessData, self).__init__()
        self.mgr = parent

    def check_jobs_statuses(self):
        """
        Checks the status of all jobs (whether still
        scraping or whether they are done)

        NOTE:
            - Prints a result to the user
            - Removes finished jobs
        """
        for job_id in list(self.mgr.jobs.keys()):
            if self.mgr.jobs[job_id].changed:
                logging.info(self.mgr.jobs[job_id])
            self.get_new_requests_from_job(job_id)
            if self.mgr.jobs[job_id].finished():
                del self.mgr.jobs[job_id]
                logging.info('Job %s has finished!', job_id)

    def get_new_requests_from_job(self, job_id):
        """ Gets and queues any new requests that jobs may have """
        for fsr in self.mgr.jobs[job_id].get_new_requests():
            self.mgr.req_queue.put(fsr)

    def run(self):
        while self.mgr.is_scraping():
            try:
                fs_req = self.mgr.resp_queue.get_nowait()
                self.mgr.jobs[fs_req.job_id].act(fs_req)
                self.mgr.jobs[fs_req.job_id].inc('responses')
            except queue.Empty:
                # time.sleep(1)
                pass
            self.check_jobs_statuses()
            if not self.mgr.jobs:
                self.mgr.stop()


class RequestIssuer(threading.Thread):
    """
    This class defines a thread that uses a FBRequestBatch object
    to aggregate and dispatch batch requests to the Graph API

    It passes the completed requests to the resp_queue so they
    can be processed
    """
    def __init__(self, parent, kwargs=None):
        """
        Initializes the thread obect for issuing graph requests.
        """
        super(RequestIssuer, self).__init__()
        self.mgr = parent

    def prepare_batch(self, fs_batch):
        """
        Prepares the batch
        """
        while (not self.mgr.req_queue.empty()) and not fs_batch.full():
            try:
                # import ipdb; ipdb.set_trace()
                fs_batch.add_request(self.mgr.req_queue.get_nowait())
            except queue.Empty:
                return fs_batch
        return fs_batch

    def run(self):
        """
        Loops until there are some requests on the queue to execute

        Responses are added to the response queue, together with the
        'to' and 'type' attributes

        NOTE: needs to reintroduce the corrected batch requests
        """
        fs_batch = FSRequestBatch(self.mgr.credentials['access_token'])
        while self.mgr.is_scraping():
            fs_batch = self.prepare_batch(fs_batch)
            if len(fs_batch._batch) != 0:
                logging.info('About to send %s requests', len(fs_batch._batch))
            fs_batch.request()
            i = 0
            for fsr in fs_batch.completed_requests():
                self.mgr.resp_queue.put(fsr)
                self.mgr.jobs[fsr.job_id].inc('responses_queued')
                i += 1
            if i != 0:
                logging.info('queued %s responses received', i)
            time.sleep(1)
