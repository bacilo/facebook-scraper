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

from csv_writer import CSVWriter
from . import Graph

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s - %(funcName)s): %(message)s',)

ISSCRAPING = True

BATCH_LIMIT = 50


class Manager(object):
    """
    This class is responsible for managing the producer/consummer threads
    """
    QUEUE_BUF_SIZE = 5000

    def __init__(self, access_token, api_key, api_secret, node_id):
        """
        Initializes the manager object and starts shared queues and
        threading objects
        """
        self.req_queue = Queue(self.QUEUE_BUF_SIZE)
        self.resp_queue = Queue(self.QUEUE_BUF_SIZE)
        self.graph = Graph(access_token=access_token, api_key=api_key,
                           api_secret=api_secret)
        self.req_queue.put(self.graph.create_page_request(node_id))
        self.proc_data = ProcessData(name='ProcessData', kwargs={
            'graph': self.graph,
            'req_queue': self.req_queue,
            'resp_queue': self.resp_queue,
            'node_id': node_id})
        self.req_issuer = RequestIssuer(name='RequestIssuer', kwargs={
            'graph': self.graph,
            'req_queue': self.req_queue,
            'resp_queue': self.resp_queue})

    def start(self):
        """Starts the threads"""
        self.req_issuer.start()
        time.sleep(2)
        self.proc_data.start()
        time.sleep(2)

    @staticmethod
    def stop():
        """Stops the threads"""
        global ISSCRAPING
        ISSCRAPING = False


class ProcessData(threading.Thread):
    """
    This class defines a thread that processes data received from the FB API
    """
    def __init__(self, group=None, target=None, name=None, kwargs=None):
        """
        Initializes the thread object for processing received data.

        TODO:
            - Receive object to parse data types received
            (e.g. 'post', 'reactions',...)
              In order to, for instance, write them to a .CSV file
        """
        super(ProcessData, self).__init__()
        self.target = target
        self.name = name
        self.graph = kwargs['graph']
        self.req_queue = kwargs['req_queue']
        self.resp_queue = kwargs['resp_queue']
        self.node_id = kwargs['node_id']
        self.csv_writer = CSVWriter(scrape_id=self.node_id)
        STATS.add_request()

    def process_post(self, posts):
        """Processes the posts received"""
        for post in posts:
            self.csv_writer.add_post(post)
            if 'comments' in post:
                self.act(post['comments'],
                         resp_type='comment',
                         resp_to=post['id'])
            if 'reactions' in post:
                self.act(post['reactions'],
                         resp_type='reaction',
                         resp_to=post['id'])
            STATS.add_post()

    def process_comments(self, comments, resp_to):
        """Processes the comments in a post"""
        for comment in comments:
            self.csv_writer.add_comment(comment, resp_to)
            STATS.add_comment()

    def process_reactions(self, reactions, resp_to):
        """Processes the reactions to a post"""
        for reaction in reactions:
            self.csv_writer.add_reaction(reaction, resp_to)
            STATS.add_reaction()

    def act(self, response, resp_type, resp_to):
        """Acts based on respose type"""
        self.find_next_request(resp=response,
                               req_type=resp_type,
                               req_to=resp_to)
        if resp_type == 'post':
            logging.error(response)
            self.process_post(response['data'])
        elif resp_type == 'comment':
            self.process_comments(response['data'], resp_to)
        elif resp_type == 'reaction':
            self.process_reactions(response['data'], resp_to)
        else:
            logging.error('response type not dealt with')

    def find_next_request(self, resp, req_type, req_to):
        """
        Tries to find the relative url for a next page requests
        """
        try:
            rel_url = resp['paging']['next'].split(
                'https://graph.facebook.com/v2.8/')[1]
            self.req_queue.put(self.graph.create_request_object(
                relative_url=rel_url,
                req_type=req_type,
                req_to=req_to))
        except KeyError:
            pass

    def run(self):
        while ISSCRAPING:
            while not self.resp_queue.empty():
                response = self.resp_queue.get()
                resp_type = response['type']
                resp_to = response['to']
                resp = json.loads(response['resp']['body'])
                self.act(response=resp, resp_type=resp_type, resp_to=resp_to)
                logging.info(STATS)


class RequestIssuer(threading.Thread):
    """
    This class defines a thread that issues batch requests to the FB API

    This class needs a Graph object to issue requests
    """
    def __init__(self, group=None, target=None, name=None, kwargs=None):
        """
        Initializes the thread obect for issuing graph requests.

        TODO:
            - Receive graph object to issue requests and receive
            (or create it and manage it here?)
        """
        super(RequestIssuer, self).__init__()
        self.target = target
        self.name = name
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
        while (
            not self.req_queue.empty() and
            len(reqs['req_batch']) < BATCH_LIMIT
                ):
            req = self.req_queue.get()
            reqs['req_batch'].append(req['req'])
            reqs['req_info'].append(
                {
                    'type': req['type'],
                    'to': req['to']
                })
            STATS.add_request()
        return reqs

    def process_responses(self, responses, req_info):
        """Processes the batch of responses received"""
        for idx, resp in enumerate(json.loads(responses)):
            self.resp_queue.put(
                {
                    'type': req_info[idx]['type'],
                    'to': req_info[idx]['to'],
                    'resp': resp
                })
            STATS.add_response()

    def run(self):
        """
        Loops until there are some requests on the queue to execute

        Responses are added to the response queue, together with the
        'to' and 'type' attributes
        """
        while ISSCRAPING:
            reqs = self.prepare_batch()
            if reqs['req_batch']:
                print(
                    'Sending batch: {} requests of types: {}'
                    .format(
                        str(len(reqs['req_batch'])),
                        reqs['req_info']))
                resp_batch = self.graph.data_request(reqs['req_batch'])
                if resp_batch:
                    self.process_responses(resp_batch.read(), reqs['req_info'])


class Stats(object):
    """
    Class that allows the keeping, and display, of scraping stats for
    logging and monitoring
    """
    def __init__(self):
        """Initialize stat object"""
        self.nbr_posts = 0
        self.nbr_comments = 0
        self.nbr_reactions = 0
        self.nbr_sharedposts = 0
        self.nbr_attachments = 0
        self.nbr_insights = 0
        self.nbr_requests = 0
        self.nbr_responses = 0

    def add_post(self):
        """Increment post count"""
        self.nbr_posts = self.nbr_posts + 1

    def add_comment(self):
        """Increment comment count"""
        self.nbr_comments = self.nbr_comments + 1

    def add_reaction(self):
        """Increment reaction count"""
        self.nbr_reactions = self.nbr_reactions + 1

    def add_sharedpost(self):
        """Increment sharedposts count"""
        self.nbr_sharedposts = self.nbr_sharedposts + 1

    def add_attachment(self):
        """Increment attachment count"""
        self.nbr_attachments = self.nbr_attachments + 1

    def add_insight(self):
        """Increment insight count"""
        self.nbr_insights = self.nbr_insights + 1

    def add_request(self):
        """Increment request count"""
        self.nbr_requests = self.nbr_requests + 1

    def add_response(self):
        """Increment response count"""
        self.nbr_responses = self.nbr_responses + 1

    def __str__(self):
        """
        User friendly display of scraping statistics
        """
        return (
            'total of {} posts, {} comments, {} reactions, {} '
            'attachments, {} shares, {} insights, {} responses, {} requests'
            ).format(
                self.nbr_posts,
                self.nbr_comments,
                self.nbr_reactions,
                self.nbr_attachments,
                self.nbr_sharedposts,
                self.nbr_insights,
                self.nbr_responses,
                self.nbr_requests)

STATS = Stats()
