# -*- coding: utf-8 -*-
"""
Defines Scraping jobs. Each scrape effort, be it a post, a group feed, etc...
is defined as a separate 'job', to keep the unity of the scraping activity
when there are multiple ones going on simultaneously
"""

import datetime
import logging
import csv_writer

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s - %(funcName)s): %(message)s',)


class Job(object):
    """
    This class defines the generic scraping effort (or 'job')
    """
    def __init__(self, job_type, node_id, callback=None):
        """
        Initalizes the Job object

        job_type: 'feed', 'post', 'page'
        node_id: the id of the node where the scraping starts
        """
        self._job_type = job_type
        self._node_id = node_id
        self._timestamp = (str(datetime.datetime.utcnow())
                           .replace(':', '_')
                           .replace('.', '_')
                           .replace(' ', '_'))
        self.callback = callback
        self.writers = dict()
        self.stats = JobStats(self.job_id)

    @property
    def job_id(self):
        """ Returns a unique job_id """
        return '{}_{}_{}'.format(
            self._timestamp,
            self._job_type,
            self._node_id)

    def register_callback(self, callback):
        self.callback = callback

    def process_feed(self, posts):
        for post in posts:
            self.writers['posts'].row(post)
            self.process_post(post)
            self.stats.add_post()

    def process_post(self, post):
        """Processes a post received"""
        if 'comments' in post:
            data = dict()
            data['resp'] = post['comments']
            data['req_type'] = 'comment'
            data['req_to'] = post['id']
            self.act(data)
        if 'reactions' in post:
            data = dict()
            data['resp'] = post['reactions']
            data['req_type'] = 'reaction'
            data['req_to'] = post['id']
            self.act(data)

    def process_comments(self, comments):
        """ Processes comments """
        for comment in comments['resp']['data']:
            comment['to_id'] = comments['req_to']
            comment['sub_comment'] = '0'
            self.writers['comments'].row(comment)
            self.stats.add_comment()
            if 'comments' in comment:
                data = dict()
                data['resp'] = comment['comments']
                data['req_type'] = 'subcomment'
                data['req_to'] = comment['id']
                self.act(data)

    def process_subcomments(self, comments):
        for comment in comments['resp']['data']:
            comment['to_id'] = comments['req_to']
            comment['sub_comment'] = '1'
            self.writers['comments'].row(comment)
            self.stats.add_sub_comment()

    def process_reactions(self, reactions):
        for reaction in reactions['resp']['data']:
            reaction['to_id'] = reactions['req_to']
            self.writers['reactions'].row(reaction)
            self.stats.add_reaction()

    def act(self, data):
        """ Acts upon received data """
        self.find_next_request(data)
        # import ipdb; ipdb.set_trace()
        try:
            if data['req_type'] == 'feed':
                self.process_feed(data['resp']['data'])
            elif data['req_type'] == 'post':
                self.process_post(data['resp'])
            elif data['req_type'] == 'comment':
                self.process_comments(data)
            elif data['req_type'] == 'subcomment':
                self.process_subcomments(data)
            elif data['req_type'] == 'reaction':
                self.process_reactions(data)
            else:
                logging.error(
                    'Error in response: %s, type: %s, to: %s (job_id = %s)',
                    data['resp'],
                    data['req_type'],
                    data['req_to'],
                    data['job_id'])
        except KeyError as kerr:
            logging.error('KeyError %s:', kerr)
            logging.error(data)
            # import ipdb; ipdb.set_trace()

    def find_next_request(self, data):
        """
        Tries to find the relative url for a next page requests
        """
        try:
            # Make this a little tidier by removing hardcoded link
            rel_url = data['resp']['paging']['next'].split(
                'https://graph.facebook.com/v2.8/')[1]
            self.callback({
                'relative_url': rel_url,
                'req_type': data['req_type'],
                'req_to': data['req_to'],
                'job_id': self.job_id
            })
            self.stats.add_request()
        except KeyError as kerr:
            # import ipdb; ipdb.set_trace()
            pass

    def finished(self):
        """ Checks if job has finished scraping """
        return self.stats.nbr_requests == self.stats.nbr_responses


class GroupJob(Job):
    """ This class implements the specific 'Group scraping job' """
    def __init__(self, node_id):
        super().__init__('feed', node_id)
        self.writers['posts'] = csv_writer.PostWriter(self.job_id)
        self.writers['reactions'] = csv_writer.ReactionWriter(self.job_id)
        self.writers['comments'] = csv_writer.CommentWriter(self.job_id)


class PostJob(Job):
    """ This class implemepnts the specific 'Post scraping job' """
    def __init__(self, node_id):
        super().__init__('post', node_id)
        self.writers['reactions'] = csv_writer.ReactionWriter(self.job_id)
        self.writers['comments'] = csv_writer.CommentWriter(self.job_id)


class JobStats(object):
    """
    Class that allows the keeping, and display, of scraping stats for
    logging and monitoring
    """
    def __init__(self, job_id):
        """Initialize stat object"""
        self.job_id = job_id
        self.nbr_posts = 0
        self.nbr_comments = 0
        self.nbr_sub_comments = 0
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

    def add_sub_comment(self):
        """Increment comment count"""
        self.nbr_sub_comments = self.nbr_sub_comments + 1

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
            'Job: {}, total: {} posts, {} comms, {} sub_comms, '
            '{} reacts, {} attachmnts, {} shares, {} insights, '
            '{} resps, {} reqs'
            ).format(
                self.job_id,
                self.nbr_posts,
                self.nbr_comments,
                self.nbr_sub_comments,
                self.nbr_reactions,
                self.nbr_attachments,
                self.nbr_sharedposts,
                self.nbr_insights,
                self.nbr_responses,
                self.nbr_requests)
