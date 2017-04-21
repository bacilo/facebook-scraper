# -*- coding: utf-8 -*-
"""Facebook Scraper Module

This module implements a graph object that can be used to query Facebook's graph API
"""
import urllib.request
import urllib.parse
import logging
import json

class Graph(object):
    """Graph object to query the Facebook graph API

    The object should be initialized with a valid Access Token.
    API Key and API Secret are optional. Should be added if an extended Access Token is wanted
    """

    FEED_FIELDS = (
        "id,created_time,picture,caption,description,from,message,message_tags,"
        "name,object_id,parent_id,shares,source,status_type,type,updated_time,with_tags"
        )
    FEED_LIMIT = 200
    REACTION_LIMIT = 100
    COMMENT_LIMIT = 50

    def __init__(self, access_token, api_key=None, api_secret=None):
        """
        Initializes the graph object and requires at least the 'Access Token'
        'Api Key' and 'Api Secret' are optional as they are only used to renew the token

        TODO: Load from config file? (Or should this be done outside?)
        """
        self.access_token = access_token
        self.api_key = api_key
        self.api_secret = api_secret

    @staticmethod
    def create_request_object(relative_url, req_type, req_to):
        """
        Creates request strings to use for batch_requests, based on relative_url

        type: can be used to determine the type of request when reading the response
        to: can be used to link certain attributes (like 'reactions') to the post they belong
        """
        return {
            'type':req_type,
            'to':req_to,
            'req': {
                "method":"GET",
                "relative_url":"{}".format(relative_url)
                }
            }

    def create_page_request(self, page_id):
        """
        Creates a request string for a page to use in batch_requests based on page_id
        """
        return self.create_request_object((
            '{}/feed?limit={}&fields={}'
            ',reactions.summary(true).limit({}){{id,name,type}}'
            ',comments.summary(true).limit({}){{id,from,message,created_time}}'
            ).format(
                page_id,
                self.FEED_LIMIT,
                self.FEED_FIELDS,
                self.REACTION_LIMIT,
                self.COMMENT_LIMIT
                ), req_type='post', req_to='')

    @staticmethod
    def encode_batch(batch):
        """
        URL encodes the batch to prepare it for a graph API request
        """
        _json = json.dumps(batch)
        _url = urllib.parse.urlparse(_json)
        return _url

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

    @staticmethod
    def request(req):
        """
        Executes a generic API request and returns the response
        Returns 'None' in case of error and logs the error
        """
        try:
            resp = urllib.request.urlopen('https://graph.facebook.com/v2.8/{}'.format(req))
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
            return

        resp = self.request(
            req='oauth/access_token?grant_type=fb_exchange_token&client_id={}'
            '&client_secret={}&fb_exchange_token={}'.format(
                self.api_key, self.api_secret, self.access_token))
        msg = json.loads(resp.read().decode('utf-8'))
        self.access_token = msg['access_token']
        return self.access_token
