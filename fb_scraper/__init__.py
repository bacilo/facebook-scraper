# -*- coding: utf-8 -*-
"""Facebook Scraper Module

This module implements a graph object that can be used
to query Facebook's graph API
"""
import logging
import json

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s - %(funcName)s): %(message)s',)


class Graph(object):
    """Graph object to query the Facebook graph API

    The object should be initialized with a valid Access Token.
    API Key and API Secret are optional. Should be added if an
    extended Access Token is wanted
    """
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

    # def data_request(self, batch_requests):
    #     """
    #     Executes a batch of requests and returns the response
    #     """
    #     json_batch = json.dumps(batch_requests)
    #     url_batch = urllib.parse.quote(json_batch)
    #     url_req = '?batch={}&access_token={}&method=POST'.format(
    #         url_batch, self.access_token)
    #     resp_batch = self.request(req=url_req)
    #     return resp_batch

    # def request(self, relative_url):
    #     """
    #     Executes a generic API request and returns the response
    #     Returns 'None' in case of error and logs the error
    #     """
    #     try:
    #         resp = urllib.request.urlopen(
    #             '{}{}'.format(
    #                 self.API_ENDPOINT,
    #                 relative_url))
    #     except urllib.error.HTTPError as httpe:
    #         logging.error(httpe)
    #         logging.error(httpe.headers)
    #         return None
    #     return resp

    # def extend_token(self):
    #     """
    #     Extends access token and replaces the previously used one
    #     Prints error message if API Key or API Secret not found

    #     TODO: Replace also config file once that file is defined
    #     TODO: Additional checks on the response
    #     """
    #     if not self.api_key or not self.api_secret:
    #         logging.error('No API Key and/or API Secret defined')
    #         return None

    #     resp = self.request(
    #         req='oauth/access_token?grant_type=fb_exchange_token&client_id={}'
    #         '&client_secret={}&fb_exchange_token={}'.format(
    #             self.api_key, self.api_secret, self.access_token))
        
    #     msg = json.loads(resp.read().decode('utf-8'))
    #     self.access_token = msg['access_token']
    #     logging.info('Extended Access Token: \n%s', self.access_token)
    #     return self.access_token
