# -*- coding: utf-8 -*-
"""
Defines Scraping jobs. Each scrape effort, be it a post, a group feed, etc...
is defined as a separate 'job', to keep the unity of the scraping activity
when there are multiple ones going on simultaneously
"""

import datetime
import logging
# from collections import defaultdict
import csv_writer
import fb_scraper

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
        self.stats['requests'] = 0  # All jobs start with one request
        self.changed = False

    def inc(self, indicator):
        """
        Increments a given indicator
        Or creates it at its first apparition
        """
        if indicator not in self.stats:
            self.stats[indicator] = 1
        else:
            self.stats[indicator] += 1
        self.changed = True

    def has_changed(self):
        """ checks if indicators havechanged """
        return self.changed

    def __str__(self):
        """ produces a string with all the indicators """
        self.changed = False
        return ''.join(
            ['%s %s,' % (value, key) for (key, value) in self.stats.items()])


class Job(JobStats):
    """
    This class defines the generic scraping effort (or 'job')
    """
    def __init__(self, job_type, node_id):
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
        self.writers = dict()
        self.abrupt_ending = False
        self.max_posts = 100000000
        self.new_requests = []

    @property
    def job_id(self):
        """ Returns a unique job_id """
        return '{}_{}_{}'.format(
            self._timestamp,
            self._job_type,
            self._node_id)

    @property
    def node_id(self):
        return self._node_id

    def get_new_requests(self):
        """ Gets a new request in LIFO fashion """
        if len(self.new_requests) == 0:
            return []
        _reqs = list(self.new_requests)
        self.new_requests = []
        return _reqs

    @staticmethod
    def build_req(resp, meta, params):
        """ Builds the request data structure """
        return fb_scraper.FSRequestSub(meta=meta, params=params, resp=resp)

    def check_for_edge(self, edge, parent_edge, params, meta):
        """
        Checks if a given post or comment,... contains
        edges such as 'reactions' or 'comments' and
        acts for it
        """
        if edge in parent_edge:
            self.act(self.build_req(
                resp=parent_edge[edge],
                meta={
                    'req_type': edge,
                    'req_to': parent_edge['id'],
                    'job_id': meta['job_id']},
                params=params
                ))
            return True
        return False

    def act(self, data):
        """
        Abstract method for others to inherit, determines how the data
        scraped is to be treated
        """
        raise NotImplementedError

    def seed(self):
        raise NotImplementedError

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
        if self.abrupt_ending and ('feed' in data.req_type):
            return
        try:
            url = data.resp['paging']['next']
            self.new_requests.append(fb_scraper.FSRequestNextPage(
                meta={
                    'req_type': data.req_type,
                    'req_to': data.req_to,
                    'job_id': self.job_id
                },
                params=data.params,
                url=url))
            self.inc('requests')
        except KeyError:
            # there is no 'next' 'paging' to follow on this dataset
            pass
        except TypeError:
            # Careful with this one, but basically some attributes like
            # 'parent_id' are being treated this way for getting the Graph
            # This means that this is also not an issue, but still to be
            # fully tested
            pass

    def finished(self):
        """ Checks if job has finished scraping """
        return self.stats['requests'] == self.stats['responses']

    def __str__(self):
        """ User friendly string with current status of Job """
        return 'Job {}_{}, total: {}'.format(
            self._job_type, self.node_id, super(Job, self).__str__())


class FeedJob(Job):
    """
    Abstracts some common aspects of scraping pages and groups and other
    elements which start by scraping the 'feed'
    """
    def __init__(self, job_type, node_id, max_posts=10000000, since=None, until=None):
        super().__init__(job_type, node_id)
        self.max_posts = max_posts
        self.writers['posts'] = csv_writer.PostWriter(self.job_id)
        self.writers['reactions'] = csv_writer.ReactionWriter(self.job_id)
        self.writers['comments'] = csv_writer.CommentWriter(self.job_id)
        self.writers['attachments'] = csv_writer.AttachmentWriter(self.job_id)
        self.writers['sharedposts'] = csv_writer.SharedPostsWriter(self.job_id)
        self.since = since
        self.until = until
        self.seed()

    def seed(self):
        """ Puts first request that launches the job """
        self.inc('requests')
        meta = {
            'req_type': self._job_type,
            'req_to': '',
            'job_id': self.job_id}
        params = {
            'since': self.since,
            'until': self.until,
            'node_id': self.node_id,
            'fs_fields': []}
        fsf = fb_scraper.FSRequestFeed(meta=meta, params=params)
        fsf.default()
        self.new_requests.append(fsf)

    def process_group_feed(self, posts, params, meta):
        """
        Processes a group feed response
        """
        for post in posts:
            self.writers['posts'].row(post)
            self.process_post(post, params=params, meta=meta)
            self.inc('posts')
            if self.stats['posts'] >= self.max_posts:
                self.abrupt_ending = True
                return

    def process_page_feed(self, posts, params, meta):
        """
        Processes a page feed response
        Similar to process_group_feed as not added specifics
        of pages
        """
        for post in posts:
            self.writers['posts'].row(post)
            self.process_post(post, params=params, meta=meta)
            self.inc('posts')
            if self.stats['posts'] >= self.max_posts:
                self.abrupt_ending = True
                return

    def process_post(self, post, params, meta):
        """Processes a post received"""
        self.check_for_edge('comments', post, params, meta)
        self.check_for_edge('reactions', post, params, meta)
        self.check_for_edge('attachments', post, params, meta)
        self.check_for_edge('sharedposts', post, params, meta)

    def process_results(self, results):
        """
        This method refactors previous methods for dealing with
        comments, reactions and attachments separately.

        NOTE: Could not integrate 'comments' quite yet given
        the specificity
        """
        for res in results.resp['data']:
            res['to_id'] = results.req_to
            self.writers[results.req_type].row(res)
            self.inc(results.req_type)

    def process_comments(self, comments, params, meta):
        """ method to process any comments or sub-comments """
        for comment in comments.resp['data']:
            comment['to_id'] = comments.req_to
            comment['comm_type'] = self.is_sub_comment(comment)
            self.writers['comments'].row(comment)
            self.check_for_edge('comments', comment, params, meta)
            self.check_for_edge('reactions', comment, params, meta)

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
            if data.req_type == 'group_feed':
                self.process_group_feed(
                    data.resp['data'], meta=data.meta, params=data.params)
            elif data.req_type == 'page_feed':
                self.process_page_feed(
                    data.resp['data'], meta=data.meta, params=data.params)
            elif data.req_type == 'post':
                self.process_post(
                    data.resp, meta=data.meta, params=data.params)
            elif data.req_type == 'comments':
                self.process_comments(
                    data, meta=data.meta, params=data.params)
            elif data.req_type in ['reactions', 'attachments', 'sharedposts']:
                self.process_results(data)
            else:
                logging.error(
                    'Error in response: %s, type: %s, to: %s (job_id = %s)',
                    data.resp,
                    data.req_type,
                    data.req_to,
                    data.job_id)
        except KeyError as kerr:
            # import ipdb; ipdb.set_trace()
            logging.error('KeyError %s:', kerr)
            logging.error(data.resp)
            logging.error(data.relative_url)


class GraphFromPageJob(Job):
    """
    This class implements a Job whose goal is to find which pages
    are connected to which other pages via their 'sharedposts',
    'attachments' and 'parent_id' fields on their posts

    Parameters:
    - levels: the number of levels to go away from the original page
    (i.e. 1, only those directed connected and 2, those connected
    to the original page plus those obtained from 1)
    """
    def __init__(self, node_id, max_levels, max_posts=100000000, since=None, until=None):
        super().__init__('graph_from_page', node_id)
        self.max_posts = max_posts
        self.max_levels = max_levels
        self.writers['pages'] = csv_writer.PageWriter(self.job_id)
        self.writers['posts'] = csv_writer.PostWriter(self.job_id)
        self.writers['sharedposts'] = csv_writer.SharedPostsWriter(self.job_id)
        self.since = since
        self.until = until
        self.groups_scraped = [node_id]
        self.seed()

    def seed(self):
        self.inc('requests')
        meta = {
            'req_type': self._job_type,
            'req_to': '',
            'job_id': self.job_id}
        params = {
            'since': self.since,
            'until': self.until,
            'node_id': self.node_id,
            'level': 0,
            'fs_fields': []}
        fsf = fb_scraper.FSRequestFeed(meta=meta, params=params)
        fsf.share_graph()
        self.new_requests.append(fsf)

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
            if data.req_type == 'graph_from_page':
                self.process_page_feed(
                    data.resp['data'], meta=data.meta, params=data.params)
            elif data.req_type == 'parent_id':
                self.process_parent_id(data)
            elif data.req_type == 'sharedposts':
                self.process_sharedposts(data)
            else:
                logging.error(
                    'Error in response: %s, type: %s, to: %s (job_id = %s)',
                    data.resp,
                    data.req_type,
                    data.req_to,
                    data.job_id)
        except KeyError as kerr:
            # import ipdb; ipdb.set_trace()
            logging.error('KeyError %s:', kerr)
            logging.error(data.resp)
            logging.error(data.relative_url)

    def process_page_feed(self, posts, meta, params):
        """
        Processes a page feed response
        Similar to process_group_feed as not added specifics
        of pages
        """
        for post in posts:
            if self.process_post(post, params=params, meta=meta):
                self.writers['posts'].row(post)
                self.inc('relevant_posts')
            self.inc('all_posts')
            if self.stats['all_posts'] >= self.max_posts:
                self.abrupt_ending = True
                return

    def process_post(self, post, meta, params):
        """Processes a post received"""
        return (
            self.check_for_edge(
                'sharedposts', post, params=params, meta=meta) or
            self.check_for_edge(
                'parent_id', post, params=params, meta=meta))

    def process_sharedposts(self, results):
        """
        This method refactors previous methods for dealing with
        comments, reactions and attachments separately.

        NOTE: Could not integrate 'comments' quite yet given
        the specificity
        """
        # import ipdb; ipdb.set_trace()
        for res in results.resp['data']:
            res['to_id'] = results.req_to
            self.writers[results.req_type].row(res)
            self.inc(results.req_type)
            if results.params['level'] >= self.max_levels:
                return
            meta = {
                'req_type': self._job_type,
                'req_to': '',
                'job_id': self.job_id}
            # import ipdb; ipdb.set_trace()
            params = {
                'since': results.params['since'],
                'until': results.params['until'],
                'node_id': res['from']['id'],
                'fs_fields': [],
                'level': results.params['level'] + 1}
            if params['node_id'] in self.groups_scraped:
                return
            self.groups_scraped.append(params['node_id'])
            self.inc('requests')
            logging.info("sharedposts: %s, from post %s, in job %s (level %s)",
                         params['node_id'],
                         results.meta['req_to'],
                         meta['job_id'],
                         params['level'])
            fsf = fb_scraper.FSRequestFeed(meta=meta, params=params)
            fsf.share_graph()
            self.new_requests.append(fsf)

    def process_parent_id(self, results):
        """
        Goes upstream once a parent_id is found on a post
        """
        # import ipdb; ipdb.set_trace()
        if results.params['level'] >= self.max_levels:
            return
        meta = {
            'req_type': self._job_type,
            'req_to': '',
            'job_id': self.job_id}
        # import ipdb; ipdb.set_trace()
        params = {
            'since': results.params['since'],
            'until': results.params['until'],
            'node_id': results.resp.split('_')[0],
            'fs_fields': [],
            'level': results.params['level'] + 1}
        if params['node_id'] in self.groups_scraped:
            return
        self.groups_scraped.append(params['node_id'])
        self.inc('requests')
        self.inc('parent_id')
        logging.info("found parent: %s, from post %s, in job %s (level %s)",
                     params['node_id'],
                     results.meta['req_to'],
                     meta['job_id'],
                     params['level'])
        fsf = fb_scraper.FSRequestFeed(meta=meta, params=params)
        fsf.share_graph()
        self.new_requests.append(fsf)


class PageJob(FeedJob):
    """
    This class implements the specific 'Page scraping job'

    TODO:
        - metadata
        - make sure it's getting most fields/attributes
        - in particular make sure it's getting the side 'ticker'
        where users post content
    """
    def __init__(self, node_id, max_posts=100000000, since=None, until=None):
        super().__init__('page_feed', node_id, max_posts, since, until)


class GroupJob(FeedJob):
    """
    This class implements the specific 'Group scraping job'

    TODO:
        - metadata from group:
        https://developers.facebook.com/docs/graph-api/reference/v2.9/group/
    """
    def __init__(self, node_id, max_posts=1000000000, since=None, until=None):
        super().__init__('group_feed', node_id, max_posts, since, until)


class PostJob(Job):
    """ This class implemepnts the specific 'Post scraping job' """
    def __init__(self, node_id):
        super().__init__('post', node_id)
        self.writers['reactions'] = csv_writer.ReactionWriter(self.job_id)
        self.writers['comments'] = csv_writer.CommentWriter(self.job_id)
        self.writers['attachments'] = csv_writer.AttachmentWriter(self.job_id)
        self.writers['sharedposts'] = csv_writer.SharedPostsWriter(self.job_id)
