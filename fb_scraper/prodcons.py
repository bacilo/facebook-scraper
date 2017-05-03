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
        logging.info('Extended Access Token: \n%s', self.graph.extend_token())
        self.jobs = dict()
        self.proc_data = ProcessData(
            parent=self,
            kwargs={'resp_queue': self.resp_queue})
        self.req_issuer = RequestIssuer(
            parent=self,
            kwargs={
                'graph': self.graph,
                'req_queue': self.req_queue,
                'resp_queue': self.resp_queue})
        self._isscraping = True

    def add_request(self, request):
        """
        Used as a callback passed to the Jobs so they can register requests
        """
        self.req_queue.put(self.graph.create_request_object(
            relative_url=request['relative_url'],
            req_type=request['req_type'],
            req_to=request['req_to'],
            job_id=request['job_id']))

    def scrape_group(self, group_id):
        """ Initiates the scraping of a group """
        job = GroupJob(group_id)
        job.register_callback(self.add_request)
        job.stats.add_request()
        self.jobs[job.job_id] = job
        self.req_queue.put(self.graph.create_group_request(group_id,
                                                           job.job_id))

    def scrape_post(self, post_id):
        """ Initiaties the scraping of a single post """
        job = PostJob(post_id)
        job.register_callback(self.add_request)
        job.stats.add_request()
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
    def __init__(self,
                 parent,
                 kwargs=None):
        """
        Initializes the thread object for processing received data.
        """
        super(ProcessData, self).__init__()
        self.resp_queue = kwargs['resp_queue']
        self.mgr = parent

    def run(self):
        while self.mgr.is_scraping():
            while not self.resp_queue.empty():
                resp = self.resp_queue.get()
                job_id = resp['job_id']
                self.mgr.jobs[job_id].act(resp)
                self.mgr.jobs[job_id].stats.add_response()
                for job_id in list(self.mgr.jobs.keys()):
                    logging.info(self.mgr.jobs[job_id].stats)
                    if self.mgr.jobs[job_id].finished():
                        del self.mgr.jobs[job_id]
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
        self.graph = kwargs['graph']
        self.req_queue = kwargs['req_queue']
        self.resp_queue = kwargs['resp_queue']

    def prepare_batch(self):
        """
        Prepares the next batch of requests

        It saves the type of request issued, together with the target of the
        request object (e.g. if requesting reactions, the 'to' could be the
        post that the reactions are from)
        """
        reqs = dict()
        reqs['req_batch'] = []
        reqs['req_info'] = []
        while (not self.req_queue.empty() and
               len(reqs['req_batch']) < self._BATCH_LIMIT):
            req = self.req_queue.get()
            reqs['req_batch'].append(req['req'])
            reqs['req_info'].append(
                {
                    'req_type': req['req_type'],
                    'req_to': req['req_to'],
                    'job_id': req['job_id']
                })
        return reqs

    def process_responses(self, responses, req_info):
        """Processes the batch of responses received"""
        for idx, resp in enumerate(json.loads(responses)):
            self.resp_queue.put(
                {
                    'req_type': req_info[idx]['req_type'],
                    'req_to': req_info[idx]['req_to'],
                    'job_id': req_info[idx]['job_id'],
                    'resp': json.loads(resp['body'])
                })

    def run(self):
        """
        Loops until there are some requests on the queue to execute

        Responses are added to the response queue, together with the
        'to' and 'type' attributes
        """
        while self.mgr.is_scraping():
            reqs = self.prepare_batch()
            if reqs['req_batch']:
                logging.info('Sending batch with: %d requests',
                             len(reqs['req_batch']))
                resp_batch = self.graph.data_request(reqs['req_batch'])
                if resp_batch:
                    self.process_responses(resp_batch.read(), reqs['req_info'])
