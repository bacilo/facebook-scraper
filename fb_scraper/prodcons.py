# -*- coding: utf-8 -*-
"""Producer/Consummer pool

Multithreading processing of responses from batch requests and issues more
requests accordingly
"""
from multiprocessing import Queue as Queue
import threading
import time
import logging
import json

from fb_scraper import Graph
from .job import GroupJob, PostJob

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s - %(funcName)s): %(message)s',)


class Manager(object):
    """
    This class is responsible for managing the producer/consummer threads
    """
    _QUEUE_BUF_SIZE = 5000

    def __init__(self, access_token, api_key, api_secret):
        """
        Initializes the manager object and starts shared queues and
        threading objects
        """
        self.req_queue = Queue(self._QUEUE_BUF_SIZE)
        self.resp_queue = Queue(self._QUEUE_BUF_SIZE)
        self.graph = Graph(access_token=access_token,
                           api_key=api_key,
                           api_secret=api_secret)
        self.graph.extend_token()
        self.proc_data = ProcessData(parent=self)
        self.req_issuer = RequestIssuer(parent=self)
        self.jobs = dict()
        self._isscraping = True

    def add_request(self, request):
        """
        Used as a callback passed to the Jobs so they can register requests
        """
        self.req_queue.put(self.graph.create_request_object(
            rel_url=request['url'].split(self.graph.API_ENDPOINT)[1],
            req_type=request['req_type'],
            req_to=request['req_to'],
            job_id=request['job_id']))

    def scrape_group(self, group_id, since=None, until=None, max_posts=100000):
        """ Initiates the scraping of a group """
        job = GroupJob(group_id, self.add_request, max_posts)
        self.jobs[job.job_id] = job
        self.req_queue.put(
            self.graph.create_group_request(group_id=group_id,
                                            job_id=job.job_id,
                                            since=since,
                                            until=until))

    def scrape_page(self, page_id, since=None, until=None, max_posts=100000):
        """ Initiates the scraping of a page """
        job = GroupJob(page_id, self.add_request, max_posts)
        self.jobs[job.job_id] = job
        self.req_queue.put(
            self.graph.create_page_request(page_id=page_id,
                                           job_id=job.job_id,
                                           since=since,
                                           until=until))

    def scrape_post(self, post_id):
        """ Initiaties the scraping of a single post """
        job = PostJob(post_id, self.add_request)
        self.jobs[job.job_id] = job
        self.req_queue.put(self.graph.create_post_request(post_id,
                                                          job.job_id))

    def start(self):
        """Starts the threads"""
        self.req_issuer.start()
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
            logging.info(self.mgr.jobs[job_id])
            if self.mgr.jobs[job_id].finished():
                del self.mgr.jobs[job_id]
                logging.info('Job %s has finished!', job_id)

    def process_response(self, response):
        """
        Takes a response and gets the appropriate job
        to deal with it

        It also increments the stats count for 'responses'
        """
        job_id = response['job_id']
        self.mgr.jobs[job_id].act(response)
        self.mgr.jobs[job_id].inc('responses')

    def run(self):
        while self.mgr.is_scraping():
            while not self.mgr.resp_queue.empty():
                self.process_response(self.mgr.resp_queue.get())
                self.check_jobs_statuses()
                if not self.mgr.jobs:
                    self.mgr.stop()


class RequestIssuer(threading.Thread):
    """
    This class defines a thread that issues batch requests to the FB API

    This class needs a Graph object to issue requests
    """
    _BATCH_LIMIT = 50

    def __init__(self, parent, kwargs=None):
        """
        Initializes the thread obect for issuing graph requests.
        """
        super(RequestIssuer, self).__init__()
        self.mgr = parent

    def prepare_batch(self):
        """
        Prepares the batch
        """
        batch = []
        while((not self.mgr.req_queue.empty()) and
              (len(batch) < self._BATCH_LIMIT)):
            batch.append(self.mgr.req_queue.get())
        return batch

    @staticmethod
    def batch_list(batch):
        """
        Returns a list with all the batch requests
        """
        return [r['req'] for r in batch]

    def queue_responses(self, api_response, batch):
        """
        Places the received responses from the graph API batch call into
        the synchronized Queue used to share data between threads
        """
        for idx, resp in enumerate(json.loads(api_response)):
            batch[idx]['resp'] = json.loads(resp['body'])
            self.mgr.resp_queue.put(batch[idx])

    @staticmethod
    def _str_req_types(batch):
        """
        Returns a string with all the request types sent in
        the batch (for user information purposes)
        """
        return ','.join([r['req_type'] for r in batch])

    def user_info(self, batch):
        """
        Displays user information pertaining to the current batch
        of requests
        """
        logging.info('Sending batch with: %d requests of types: %s',
                     len(batch),
                     self._str_req_types(batch))

    def run(self):
        """
        Loops until there are some requests on the queue to execute

        Responses are added to the response queue, together with the
        'to' and 'type' attributes
        """
        while self.mgr.is_scraping():
            batch = self.prepare_batch()
            if batch:
                self.user_info(batch)
                api_resp = self.mgr.graph.data_request(self.batch_list(batch))
                self.queue_responses(api_resp.read(), batch)
