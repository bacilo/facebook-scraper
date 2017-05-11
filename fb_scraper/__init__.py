# -*- coding: utf-8 -*-
"""
Defines Request and Response object wrappers to deal with most of the
functionality desired from these objects.

It acts as an interface between the different systems in the project, such
as the graph API wrapper and the prodcons.py consummer/producer object
"""
import urllib.request
import urllib.parse
import logging
import json
import re

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s - %(funcName)s): %(message)s',)


class FSField(object):
    """
    Simple class to define fields
    """
    REACTION_LIMIT = 50
    COMMENT_LIMIT = 50

    def __init__(self, limit=None):
        self._limit = limit
        self._attribs = []
        self._sub_fields = []

    def __str__(self):
        raise NotImplementedError

    @property
    def limit(self):
        """ Returns a string for setting limit """
        if self._limit:
            return '.limit({})'.format(self._limit)
        else:
            return ''

    @property
    def summary(self):
        """ Returns a string requesting for a summary """
        return '.summary(true)'

    @property
    def attribs(self):
        """ Returns a string joining all the attributes """
        return ','.join(self._attribs)

    @property
    def sub_fields(self):
        """ Returns a string joining all the sub_fields """
        return ','.join([str(fs_field) for fs_field in self._sub_fields])

    @property
    def fields(self):
        """ Returns a string joining attribs and sub_fields """
        if not self.attribs:
            return '{}'.format(self.sub_fields)
        if not self.sub_fields:
            return '{}'.format(self.attribs)
        return '{},{}'.format(self.attribs, self.sub_fields)


class FSFieldComments(FSField):
    """
    Class for comments field
    """
    FIELD_ATTRIBS = ['id', 'from', 'message', 'created_time', 'like_count',
                     'attachment']

    def __init__(self, limit, sub_fields):
        super().__init__(limit)
        self._attribs.extend(self.FIELD_ATTRIBS)
        self._sub_fields.extend(sub_fields)

    def default(self):
        """
        Loads FSFieldComments with default values (i.e. reactions and
        subcomments)
        """
        comm_reactions = FSFieldReactions(
            limit=self.REACTION_LIMIT,
            sub_fields=[])
        sub_comm_reactions = FSFieldReactions(
            limit=self.REACTION_LIMIT,
            sub_fields=[])
        sub_comms = FSFieldComments(
            limit=self.COMMENT_LIMIT,
            sub_fields=[sub_comm_reactions])
        self._sub_fields.extend([comm_reactions, sub_comms])

    def __str__(self):
        return 'comments{}{}{{{}}}'.format(
            self.summary,
            self.limit,
            self.fields)


class FSFieldReactions(FSField):
    """
    Class for reactions field
    """
    FIELD_ATTRIBS = ['id', 'name', 'type']

    def __init__(self, limit, sub_fields):
        super().__init__(limit)
        self._attribs.extend(self.FIELD_ATTRIBS)
        self._sub_fields.extend(sub_fields)

    def __str__(self):
        return 'reactions{}{}{{{}}}'.format(
            self.summary,
            self.limit,
            self.fields)


class FSFieldAttachments(FSField):
    """
    Class for attachments field
    """
    FIELD_ATTRIBS = ['description', 'description_tags', 'media', 'target',
                     'title', 'type', 'url']

    def __init__(self):
        super().__init__()
        self._attribs.extend(self.FIELD_ATTRIBS)

    def __str__(self):
        return 'attachments{{{}}}'.format(
            self.attribs)


class FSFieldSharedPosts(FSField):
    """
    Class for sharedposts field

    NOTE:
        Supposedly we could get the same fields than for any other post
        Not sure yet what to get from this at this stage...
    """
    FIELD_ATTRIBS = ['id', 'from', 'to', 'story', 'created_time',
                     'updated_time']

    def __init__(self):
        super().__init__()
        self._attribs.extend(self.FIELD_ATTRIBS)

    def __str__(self):
        return 'sharedposts{{{}}}'.format(
            self.attribs)


class FSRequest(object):
    """
    This class wraps the object used as a request object
    """
    API_ENDPOINT = 'https://graph.facebook.com/v2.9/'
    POST_ATTRIBS = ["id", "created_time", "picture", "caption", "description",
                    "from", "message", "message_tags", "name", "object_id",
                    "parent_id", "shares", "source", "status_type", "type",
                    "updated_time", "with_tags"]
    FEED_LIMIT = 100

    def __init__(self, meta, params, access_token=None):
        self.meta = meta
        self.params = params
        self._attribs = []
        self.resp = None
        self.access_token = access_token
        self._limit = self.FEED_LIMIT

    def to_batch(self):
        """ Returns dict object for batch requests """
        return {
            'method': 'GET',
            'relative_url': '{}'.format(self.relative_url)
        }

    @property
    def relative_url(self):
        """ Gets the relative URL (i.e. without the endpoint) """
        raise NotImplementedError

    @property
    def url(self):
        """ Gets the fuill URL (i.e. with the endpoint) """
        return '{}{}'.format(self.API_ENDPOINT, self.relative_url)

    @property
    def req_type(self):
        """ Returns request type """
        return self.meta['req_type']

    @property
    def req_to(self):
        """ Returns request to """
        return self.meta['req_to']

    @property
    def job_id(self):
        """ Returns job id """
        return self.meta['job_id']

    @property
    def attribs(self):
        """ Returns attribute list ready for request concatenation """
        return ','.join(self._attribs)

    @property
    def fields(self):
        """ Returns fields list ready for request concatenation """
        return '&fields={},{}'.format(self.attribs, self.fs_fields)

    @property
    def node_id(self):
        """ Returns node id """
        return self.params['node_id']

    @property
    def fs_fields(self):
        """ Returns all included FSRequests as fields """
        return ','.join(
            [str(fs_field) for fs_field in self.params['fs_fields']])

    @property
    def str_limit(self):
        """
        Returns a string denoting the 'limit' attribute, ready for
        concatenation in the request
        """
        return 'limit={}'.format(self._limit)

    @property
    def limit(self):
        """ Getter for limit field """
        return self._limit

    @limit.setter
    def limit(self, val):
        self._limit = val

    def correct_request(self):
        """
        Should correct a request which was not concluded
        for instance because too much data was being requested
        """
        error_msg = "Please reduce the amount of data you're asking"
        if 'error' in self.resp:
            if 'message' in self.resp['error']:
                if error_msg in self.resp['error']['message']:
                    self.change_feed_limit()
                    logging.info(
                        '{} asking for too much data, reduced to: {}'.format(
                            self.job_id,
                            self.limit))

    def change_feed_limit(self, factor=0.5):
        """
        Changes the limit value for a feed in a request

        NOTE: for this to work on FSRequestSub or FSRequestNextPage both
        classes need to be tuned to take in the limit as a value and
        build relative_url (rather than just having relative_url stored)
        """
        self.limit = int(self.limit*factor)

    def pre_request(self):
        """
        To be overriden once a subclass wants to execute something
        before a request is carried out
        """
        pass

    def post_request(self):
        """
        To be overriden once a subclass wants to execute something
        after a request is carried out
        """
        pass

    def request(self):
        """
        Performs an http request.
        It updates FSRequest with the response
        """
        # import ipdb; ipdb.set_trace()
        self.pre_request()
        try:
            self.resp = urllib.request.urlopen('{}{}'.format(
                self.API_ENDPOINT,
                self.relative_url))
        except urllib.error.HTTPError as httpe:
            logging.error(httpe)
            logging.error(httpe.headers)
            self.resp = None
        self.post_request()


class FSRequestSub(FSRequest):
    """
    This is a class used to encompass some sub_requests for scraping
    It is NOT intended as a general 'request' class, but just as an
    intermediary one when scraping, for consistence purposes

    (i.e. to keep the 'comments' or 'reactions' that are in the response
    for a post, for instance)
    """
    def __init__(self, meta, resp):
        super().__init__(meta=meta, params=None)
        self.resp = resp


class FSRequestNextPage(FSRequest):
    """
    This class implements a simple wrapper for a next page request
    that can be of comments, reactions, posts,...
    """
    def __init__(self, meta, url):
        super().__init__(meta=meta, params=None)
        self._url = url

    @property
    def relative_url(self):
        return self._url.split(self.API_ENDPOINT)[1]

    def change_feed_limit(self, factor=0.5):
        """
        Changes the limit value for a feed in a request
        This is used for this one because it's not properly implemented
        In the future the idea is to change so the url is parsed into the
        relevant fields and returned in the relative_url in proper fashion
        """
        val = re.split(r'(limit=)(\d*)', self.relative_url)
        # import ipdb; ipdb.set_trace()
        try:
            val[2] = str(int(int(val[2])*factor))
            if int(val[2]) < 1:  # Make sure limit is not set to 0
                val[2] = 1
        except IndexError as ie:
            logging.error('could not change limit: %s', ie)
        self.relative_url = ''.join(val)


class FSRequestBatch(FSRequest):
    """
    This class implements a FSRequest object for a batch
    """
    BATCH_LIMIT = 50

    def __init__(self, access_token):
        super().__init__(meta=None, params=None, access_token=access_token)
        self._batch = []

    def add_request(self, fs_request):
        """
        Adds a FSRequest object to the batch

        Returns:
            - True if added
            - False if full
        """
        if len(self._batch) < self.BATCH_LIMIT:
            self._batch.append(fs_request)
            return True
        else:
            return False

    def full(self):
        """ Checks if batch is full """
        return len(self._batch) == self.BATCH_LIMIT

    def empty(self):
        """ Checks if the batch is empty """
        return len(self._batch) == 0

    @property
    def batch_json(self):
        """ Turns batch into a json object """
        return json.dumps([r.to_batch() for r in self._batch])

    @property
    def relative_url(self):
        return '?batch={}&access_token={}&method=POST'.format(
            urllib.parse.quote(self.batch_json),
            self.access_token)

    def pre_request(self):
        return self.empty()  # If it is empty this will cancel the request

    def post_request(self):
        """
        This is executed to update the response on each of the
        individual FSRequests that compose this batch

        This batch can then be added directly to the list of
        responses.
        NOTE: some requests might be invalid, like requesting
        too much data
        """
        for idx, resp in enumerate(json.loads(self.resp.read())):
            self._batch[idx].resp = json.loads(resp['body'])

    def correct_requests(self):
        """ Corrects all requests so they can be sent again """
        for idx, fsr in enumerate(self._batch):
            self._batch[idx].correct_request()

    def completed_requests(self):
        """
        Returns all completed requests and removes them from the batch
        """
        _done = []
        for idx, fsr in enumerate(self._batch):
            if 'error' not in fsr.resp:
                _done.append(fsr)
                self._batch.pop(idx)
        self.correct_requests()
        # import ipdb; ipdb.set_trace()
        return _done

    def _str_types(self):
        """
        Returns a string with all the request types sent in
        the batch (for user information purposes)
        """
        return ','.join([r['req_type'] for r in self._batch])

    def __str__(self):
        return 'Batch has: {} requests of types: [{}]'.format(
            len(self._batch),
            self._str_types())


class FSRequestExtendAccessToken(FSRequest):
    """
    This class implements a FSRequest for an extended access token
    """
    def __init__(self, access_token, client_id=None, client_secret=None):
        super().__init__(meta=None, params=None, access_token=access_token)
        self.client_id = client_id
        self.client_secret = client_secret

    @property
    def relative_url(self):
        return ('oauth/access_token?grant_type=fb_exchange_token&client_id={}'
                '&client_secret={}&fb_exchange_token={}').format(
                    self.client_id,
                    self.client_secret,
                    self.access_token)

    def pre_request(self):
        if not self.client_id or not self.client_secret:
            logging.error('No API Key and/or API Secret defined')
            return True

    def post_request(self):
        new_tok = json.loads(self.resp.read().decode('utf-8'))['access_token']
        logging.info('Extended Access Token: \n%s', new_tok)


class FSRequestPost(FSRequest):
    """
    This class implements a FSRequest object for a post

    Arguments:
    - meta:
        Dictionary containing entries to:
            - 'req_type'
            - 'req_to'
            - 'job_id'
    - params:
        Dictionary containing the search parameters:
            - 'node_id': the node for which to get the feed from
            - 'fs_fields': list with FSFields to consider
    """
    def __init__(self, meta, params):
        super().__init__(meta=meta, params=params)
        self._attribs.extend(self.POST_ATTRIBS)

    @property
    def relative_url(self):
        return '{}?{}'.format(
            self.node_id,
            self.fields)


class FSRequestFeed(FSRequest):
    """
    This class implements a FSRequest object for a feed

    Arguments:
    - meta:
        Dictionary containing entries to:
            - 'req_type'
            - 'req_to'
            - 'job_id'
    - params:
        Dictionary containing the search parameters:
            - 'since'
            - 'until'
            - 'node_id': the node for which to get the feed from
            - 'fs_fields': list with FSFields to consider
    """
    def __init__(self, meta, params):
        super().__init__(meta=meta, params=params)
        self._attribs.extend(self.POST_ATTRIBS)

    @property
    def since(self):
        """
        Returns a string denoting the 'since' attribute, ready for
        concatenation in the request
        """
        return '&since={}'.format(
            self.params['since']) if self.params['since'] else ''

    @property
    def until(self):
        """
        Returns a string denoting the 'until' attribute, ready for
        concatenation in the request
        """
        return '&until={}'.format(
            self.params['until']) if self.params['until'] else ''

    def default(self):
        """
        Initializes feed with 'default' FSFields (i.e. comments, subcomments,
        reactions, attachments, sharedposts)
        """
        comms = FSFieldComments(
            limit=FSField.COMMENT_LIMIT,
            sub_fields=[])
        comms.default()
        reactions = FSFieldReactions(
            limit=FSField.REACTION_LIMIT,
            sub_fields=[])
        attachments = FSFieldAttachments()
        sharedposts = FSFieldSharedPosts()
        self.params['fs_fields'].extend(
            [comms, reactions, attachments, sharedposts])

    @property
    def relative_url(self):
        return '{}/feed?{}{}{}{}'.format(
            self.node_id,
            self.str_limit,
            self.since,
            self.until,
            self.fields)
