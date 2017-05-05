# -*- coding: utf-8 -*-
"""Facebook Scraper Module

This module implements a graph object that can be used
to query Facebook's graph API
"""
import urllib.request
import urllib.parse
import logging
import json


class Graph(object):
    """Graph object to query the Facebook graph API

    The object should be initialized with a valid Access Token.
    API Key and API Secret are optional. Should be added if an
    extended Access Token is wanted
    """

    API_ENDPOINT = 'https://graph.facebook.com/v2.9/'
    FEED_FIELDS = (
        "id,created_time,picture,caption,description,from,message,"
        "message_tags,name,object_id,parent_id,shares,source,"
        "status_type,type,updated_time,with_tags"
        )
    FEED_LIMIT = 50
    REACTION_LIMIT = 100
    COMMENT_LIMIT = 50

    def __init__(self, access_token, api_key=None, api_secret=None):
        """
        Initializes the graph object and requires at least the 'Access Token'
        'Api Key' and 'Api Secret' are optional as they are only used to
        renew the token

        TODO: Load from config file? (Or should this be done outside?)
        """
        self.access_token = access_token
        self.api_key = api_key
        self.api_secret = api_secret

    @staticmethod
    def create_request_object(rel_url, req_type, req_to, job_id):
        """
        Creates request strings to use for batch_requests,
        based on rel_url

        type: can be used to determine the type of request when reading
            the response
        to: can be used to link certain attributes (like 'reactions')
            to the post they belong
        """
        # print(rel_url)
        return {
            'req_type': req_type,
            'req_to': req_to,
            'job_id': job_id,
            'req': {
                "method": "GET",
                "relative_url": "{}".format(rel_url)
                }
            }

    @staticmethod
    def str_sharedposts_query():
        """
        String for querying sharedposts
        NOTE:
        Supposedly we could get the same fields than for any other post
        Not sure yet what to get from this at this stage...
        """
        return 'sharedposts{id,from,to,story,created_time,updated_time}'

    @staticmethod
    def str_attachments_query():
        """String for querying attachments"""
        return ('attachments{description, description_tags, media,'
                'target, title, type, url}')

    def str_reactions_query(self):
        """String for querying reactions"""
        return 'reactions.summary(true).limit({}){{id,name,type}}'.format(
            self.REACTION_LIMIT)

    def str_sub_comments_query(self):
        """
        String for querying sub-comments
        Note: Limit could be defined for this
        """
        return ('comments{{id,from,message,created_time,'
                'like_count,{}}}').format(self.str_reactions_query())

    def str_comments_query(self):
        """String for querying comments"""
        return ('comments.summary(true).limit({}){{id,from,message,'
                'created_time,like_count,comment_count,{},{}}}').format(
                    self.COMMENT_LIMIT,
                    self.str_reactions_query(),
                    self.str_sub_comments_query())

    def create_group_request(
            self,
            group_id,
            job_id,
            since=None,
            until=None):
        """
        Creates a request string for a page to use in
        batch_requests based on page_id

        Since/Until fields:
            Can be empty, or a str of one the two forms
            YYYY-MM-DD
            YYYY-MM-DDTHH:MM:SS
        """
        since_str = ''
        until_str = ''
        if since:
            since_str = '&since={}'.format(since)
        if until:
            until_str = '&until={}'.format(until)
        return self.create_request_object((
            '{}/feed?limit={}{}{}&fields={},{},{},{},{}'.format(
                group_id,
                self.FEED_LIMIT,
                since_str,
                until_str,
                self.FEED_FIELDS,
                self.str_reactions_query(),
                self.str_comments_query(),
                self.str_sharedposts_query(),
                self.str_attachments_query())),
                                          req_type='feed',
                                          req_to='',
                                          job_id=job_id)

    def create_post_request(self, post_id, job_id):
        """
        Creates a request string for a post to use in
        batch_requests based on post_id
        Note: could add limit as well?
        """
        return self.create_request_object((
            '{}?fields={},{},{},{}').format(
                post_id,
                self.str_reactions_query(),
                self.str_comments_query(),
                self.str_sharedposts_query(),
                self.str_attachments_query()),
                                          req_type='post',
                                          req_to='',
                                          job_id=job_id)

    # @staticmethod
    # def encode_batch(batch):
    #     """
    #     URL encodes the batch to prepare it for a graph API request
    #     """
    #     _json = json.dumps(batch)
    #     _url = urllib.parse.urlparse(_json)
    #     return _url

    def data_request(self, batch_requests):
        """
        Executes a batch of requests and returns the response
        """
        json_batch = json.dumps(batch_requests)
        url_batch = urllib.parse.quote(json_batch)
        url_req = '?batch={}&access_token={}&method=POST'.format(
            url_batch, self.access_token)
        resp_batch = self.request(req=url_req)
        return resp_batch

    def request(self, req):
        """
        Executes a generic API request and returns the response
        Returns 'None' in case of error and logs the error
        """
        try:
            resp = urllib.request.urlopen(
                '{}{}'.format(
                    self.API_ENDPOINT,
                    req))
        except urllib.error.HTTPError as httpe:
            logging.error(httpe)
            logging.error(httpe.headers)
            return None
        return resp

    def extend_token(self):
        """
        Extends access token and replaces the previously used one
        Prints error message if API Key or API Secret not found

        TODO: Replace also config file once that file is defined
        TODO: Additional checks on the response
        """
        if not self.api_key or not self.api_secret:
            logging.error('No API Key and/or API Secret defined')
            return None

        resp = self.request(
            req='oauth/access_token?grant_type=fb_exchange_token&client_id={}'
            '&client_secret={}&fb_exchange_token={}'.format(
                self.api_key, self.api_secret, self.access_token))
        msg = json.loads(resp.read().decode('utf-8'))
        self.access_token = msg['access_token']
        return self.access_token
