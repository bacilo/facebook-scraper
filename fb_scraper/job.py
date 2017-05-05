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
    """
    This class implements the stats counting methods and should be
    inherited by the Job classes that seek to use this functionality
    """
    def __init__(self):
        self.stats = dict()
        self.stats['responses'] = 0
        self.stats['requests'] = 1  # All jobs start with one request

    def inc(self, indicator):
        """
        Increments a given indicator
        Or creates it at its first apparition
        """
        if indicator not in self.stats:
            self.stats[indicator] = 1
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
        """ Processes a feed response """
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

    def check_for_edge(self, edge, parent_edge):
        """
        Checks if a given post or comment,... contains
        edges such as 'reactions' or 'comments' and
        acts for it
        """
        if edge in parent_edge:
            self.act(self.build_req(
                resp=parent_edge[edge],
                req_type=edge,
                req_to=parent_edge['id']
                ))

    def process_post(self, post):
        """Processes a post received"""
        self.check_for_edge('comments', post)
        self.check_for_edge('reactions', post)
        self.check_for_edge('attachments', post)
        self.check_for_edge('sharedposts', post)

    def process_results(self, results):
        """
        This method refactors previous methods for dealing with
        comments, reactions and attachments separately.

        NOTE: Could not integrate 'comments' quite yet given
        the specificity
        """
        for res in results['resp']['data']:
            res['to_id'] = results['req_to']
            self.writers[results['req_type']].row(res)
            self.inc(results['req_type'])

    def process_comments(self, comments):
        """ method to process any comments or sub-comments """
        for comment in comments['resp']['data']:
            comment['to_id'] = comments['req_to']
            comment['comm_type'] = self.is_sub_comment(comment)
            self.writers['comments'].row(comment)
            self.check_for_edge('comments', comment)
            self.check_for_edge('reactions', comment)

    def is_sub_comment(self, comment):
        """
        Quick and dirty way to check if it's a comment on a post
        or a comment to a comment (i.e. 'sub_comment')

        Basically a comment to a post will have the post_id as their
        'to_id' which will contain an underscore '_', whereas a
        comment to a comment will not
        """
        comm_type = 'sub_comm' if '_' not in comment['to_id'] else 'comm'
        self.inc(comm_type)
        return comm_type

    def act(self, data):
        """ Acts upon received data """
        self.find_next_request(data)
        # import ipdb; ipdb.set_trace()
        try:
            if data['req_type'] == 'feed':
                self.process_feed(data['resp']['data'])
            elif data['req_type'] == 'post':
                self.process_post(data['resp'])
            elif data['req_type'] == 'comments':
                self.process_comments(data)
            elif data['req_type'] == 'reactions':
                self.process_results(data)
            elif data['req_type'] == 'attachments':
                self.process_results(data)
            elif data['req_type'] == 'sharedposts':
                self.process_results(data)
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
        if self.abrupt_ending and (data['req_type'] == 'feed'):
            return
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
        return self.stats['requests'] == self.stats['responses']

    def __str__(self):
        return 'Job {}, total: {}'.format(
            self.job_id, super(Job, self).__str__())


class GroupJob(Job):
    """
    This class implements the specific 'Group scraping job'

    TODO:
        - metadata from group:
        https://developers.facebook.com/docs/graph-api/reference/v2.9/group/
    """
    def __init__(self, node_id, callback, max_posts):
        super().__init__('feed', node_id, callback)
        self.max_posts = max_posts
        self.writers['posts'] = csv_writer.PostWriter(self.job_id)
        self.writers['reactions'] = csv_writer.ReactionWriter(self.job_id)
        self.writers['comments'] = csv_writer.CommentWriter(self.job_id)
        self.writers['attachments'] = csv_writer.AttachmentWriter(self.job_id)
        self.writers['sharedposts'] = csv_writer.SharedPostsWriter(self.job_id)


class PostJob(Job):
    """ This class implemepnts the specific 'Post scraping job' """
    def __init__(self, node_id, callback):
        super().__init__('post', node_id, callback)
        self.writers['reactions'] = csv_writer.ReactionWriter(self.job_id)
        self.writers['comments'] = csv_writer.CommentWriter(self.job_id)
        self.writers['attachments'] = csv_writer.AttachmentWriter(self.job_id)
        self.writers['sharedposts'] = csv_writer.SharedPostsWriter(self.job_id)
