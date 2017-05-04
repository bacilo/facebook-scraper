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


class JobStats(object):
    def __init__(self):
        self.stats = dict()
        self.stats['responses'] = 0
        self.stats['requests'] = 0

    def inc(self, indicator):
        """increments a given indicator"""
        if indicator not in self.stats:
            self.stats[indicator] = 0
        else:
            self.stats[indicator] += 1

    def __str__(self):
        """produces a string with all the indicators"""
        return ''.join(
            ['%s %s,' % (value, key) for (key, value) in self.stats.items()])


class Job(JobStats):
    """
    This class defines the generic scraping effort (or 'job')
    """
    def __init__(self, job_type, node_id, callback=None):
        """
        Initalizes the Job object

        job_type: 'feed', 'post', 'page'
        node_id: the id of the node where the scraping starts
        """
        super().__init__()
        self.inc('requests')
        self._job_type = job_type
        self._node_id = node_id
        self._timestamp = (str(datetime.datetime.utcnow())
                           .replace(':', '_')
                           .replace('.', '_')
                           .replace(' ', '_'))
        self.callback = callback
        self.writers = dict()
        self.abrupt_ending = False
        self.max_posts = 1000000

    @property
    def job_id(self):
        """ Returns a unique job_id """
        return '{}_{}_{}'.format(
            self._timestamp,
            self._job_type,
            self._node_id)

    def process_feed(self, posts):
        for post in posts:
            self.writers['posts'].row(post)
            self.process_post(post)
            self.inc('posts')
            if self.stats['posts'] >= self.max_posts:
                self.abrupt_ending = True
                return

    @staticmethod
    def build_req(resp, req_type, req_to):
        """ Builds the request data structure """
        data = dict()
        data['resp'] = resp
        data['req_type'] = req_type
        data['req_to'] = req_to
        return data

    def process_post(self, post):
        """Processes a post received"""
        if 'comments' in post:
            self.act(self.build_req(
                post['comments'],
                'comment',
                post['id']))
        if 'reactions' in post:
            self.act(self.build_req(
                post['reactions'],
                'reaction',
                post['id']))
        if 'attachments' in post:
            for att in post['attachments']['data']:
                att['target_id'] = post['id']
                self.writers['attachments'].row(att)
                self.inc('attachments')

    def process_comments(self, comments):
        """ Processes comments """
        for comment in comments['resp']['data']:
            comment['to_id'] = comments['req_to']
            comment['sub_comment'] = '0'
            self.writers['comments'].row(comment)
            self.inc('comments')
            if 'comments' in comment:
                self.act(self.build_req(
                    comment['comments'],
                    'subcomment',
                    comment['id']))

    def process_subcomments(self, comments):
        """ Processes subcomments """
        for comment in comments['resp']['data']:
            comment['to_id'] = comments['req_to']
            comment['sub_comment'] = '1'
            self.writers['comments'].row(comment)
            self.inc('sub_comments')

    def process_reactions(self, reactions):
        """ Processes reactions """
        for reaction in reactions['resp']['data']:
            reaction['to_id'] = reactions['req_to']
            self.writers['reactions'].row(reaction)
            self.inc('reactions')

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

    def find_next_request(self, data):
        """
        Tries to find the relative url for a next page requests
        """
        try:
            # Make this a little tidier by removing hardcoded link
            url = data['resp']['paging']['next']
            self.callback({
                'url': url,
                'req_type': data['req_type'],
                'req_to': data['req_to'],
                'job_id': self.job_id
            })
            self.inc('requests')
        except KeyError:
            # import ipdb; ipdb.set_trace()
            pass

    def finished(self):
        """ Checks if job has finished scraping """
        return (self.abrupt_ending or
                (self.stats['requests'] == self.stats['responses']))

    def __str__(self):
        return 'Job {}, total: {}'.format(
            self.job_id, super(Job, self).__str__())


class GroupJob(Job):
    """ This class implements the specific 'Group scraping job' """
    def __init__(self, node_id, callback, max_posts):
        super().__init__('feed', node_id, callback)
        self.max_posts = max_posts
        self.writers['posts'] = csv_writer.PostWriter(self.job_id)
        self.writers['reactions'] = csv_writer.ReactionWriter(self.job_id)
        self.writers['comments'] = csv_writer.CommentWriter(self.job_id)
        self.writers['attachments'] = csv_writer.AttachmentWriter(self.job_id)


class PostJob(Job):
    """ This class implemepnts the specific 'Post scraping job' """
    def __init__(self, node_id, callback):
        super().__init__('post', node_id, callback)
        self.writers['reactions'] = csv_writer.ReactionWriter(self.job_id)
        self.writers['comments'] = csv_writer.CommentWriter(self.job_id)
        self.writers['attachments'] = csv_writer.AttachmentWriter(self.job_id)
