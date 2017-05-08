# -*- coding: utf-8 -*-
"""
Defines Scraping jobs. Each scrape effort, be it a post, a group feed, etc...
is defined as a separate 'job', to keep the unity of the scraping activity
when there are multiple ones going on simultaneously
"""

import datetime
import logging
from collections import defaultdict
import re
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

        job_type: 'group_feed', 'post', 'page'
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

    def process_group_feed(self, posts):
        """
        Processes a group feed response
        """
        for post in posts:
            self.writers['posts'].row(post)
            self.process_post(post)
            self.inc('posts')
            if self.stats['posts'] >= self.max_posts:
                self.abrupt_ending = True
                return

    def process_page_feed(self, posts):
        """
        Processes a page feed response
        Similar to process_group_feed as not added specifics
        of pages
        """
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
        """
        Acts upon received data
        This method receives a batch of a certain type of data and acts
        as a switch to which method will process that type of data.

        Data should be always of a 'req_type'. The 'req_type' should
        fall onto one of the defined types in the switch form.

        KeyError should also never occur.

        In both cases, logging is done to help ientify the issue
        """
        self.find_next_request(data)
        # import ipdb; ipdb.set_trace()
        try:
            if data['req_type'] == 'group_feed':
                self.process_group_feed(data['resp']['data'])
            elif data['req_type'] == 'page_feed':
                self.process_page_feed(data['resp']['data'])
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
            # import ipdb; ipdb.set_trace()
            logging.error('KeyError %s:', kerr)
            logging.error(data['resp'])
            if self.request_too_large(data):
                return self.change_feed_limit(data)

    @staticmethod
    def request_too_large(data):
        """
        Checks if the request was too large
        To be used in case of error in getting data
        """
        error_msg = "Please reduce the amount of data you're asking"
        if 'error' in data['resp']:
            if 'message' in data['resp']['error']:
                if error_msg in data['resp']['error']['message']:
                    logging.info('asking for too much data!')
                    return True
        return False

    @staticmethod
    def change_feed_limit(data, factor=0.5):
        """
        Changes the limit value for a feed in a request
        """
        val = re.split(r'(/feed\?limit=)(\d*)', data['req']['relative_url'])
        val[2] = str(int(int(val[2])*factor))
        if int(val[2]) < 1:  # Make sure limit is not set to 0
            val[2] = 1
        # import ipdb; ipdb.set_trace()
        data['req']['relative_url'] = ''.join(val)
        return data

    def find_next_request(self, data):
        """
        Tries to find the relative url for a next page requests, which
        is given usually in the entry 'next', under 'paging'

        This method is independent of whether it pertains to 'comments',
        'reactions', or any other type of data, since the attributes
        of the request are merely transfered to the response

        TODO:
            - right now it's only testing for 'group_feed'
            if limits are indicated for other feeds then those must
            be included here so the job can terminate
        """
        if self.abrupt_ending and (data['req_type'] == 'group_feed'):
            return
        try:
            url = data['resp']['paging']['next']
            self.callback({
                'url': url,
                'req_type': data['req_type'],
                'req_to': data['req_to'],
                'job_id': self.job_id
            })
            self.inc('requests')
        except KeyError:
            # there is no 'next' 'paging' to follow on this dataset
            pass

    def finished(self):
        """ Checks if job has finished scraping """
        return self.stats['requests'] == self.stats['responses']

    def __str__(self):
        """ User friendly string with current status of Job """
        return 'Job {}, total: {}'.format(
            self.job_id, super(Job, self).__str__())


class PageJob(Job):
    """
    This class implements the specific 'Page scraping job'

    TODO:
        - metadata
        - make sure it's getting most fields/attributes
        - in particular make sure it's getting the side 'ticker'
        where users post content
    """
    def __init__(self, node_id, callback, max_posts):
        super().__init__('page_feed', node_id, callback)
        self.max_posts = max_posts
        self.writers['posts'] = csv_writer.PostWriter(self.job_id)
        self.writers['reactions'] = csv_writer.ReactionWriter(self.job_id)
        self.writers['comments'] = csv_writer.CommentWriter(self.job_id)
        self.writers['attachments'] = csv_writer.AttachmentWriter(self.job_id)
        self.writers['sharedposts'] = csv_writer.SharedPostsWriter(self.job_id)


class GroupJob(Job):
    """
    This class implements the specific 'Group scraping job'

    TODO:
        - metadata from group:
        https://developers.facebook.com/docs/graph-api/reference/v2.9/group/
    """
    def __init__(self, node_id, callback, max_posts):
        super().__init__('group_feed', node_id, callback)
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


class JobManager(object):
    """
    This classes manages the individual scraping jobs.
    Mainly it allows for joint/combined operations over different jobs
    (like summing their stats for instance)

    NOTE: not being used or properly tested yet!
    """
    def __init__(self):
        self._jobs = []
        self._maxjobs = 0

    def add_job(self, job):
        self._jobs.append(job)
        self._maxjobs += 1

    def total_stats(self):
        """
        Calculates a dictionary with the total stats for all the jobs
        """
        total_d = defaultdict(int)
        for job in self._jobs:
            for key, value in job.iteritems():
                total_d[key] += value
        return total_d

    def __str__(self):
        t_s = self.total_stats()
        return '{} active jobs; {} finished jobs. INFO: {}'.format(
            len(self._jobs),
            self._maxjobs - len(self._jobs),
            ''.join(['%s %s,' % (value, key) for (key, value) in t_s.items()]))
