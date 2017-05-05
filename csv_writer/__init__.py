# -*- coding: utf-8 -*-
"""CSV Writer

This class will be dumping the scraped data to different CSV files

NOTE: must deal with encoding('utf-8') as it seems to encode foreign
characters, emoticons, etc...
Removing it though, at least in Excel importing, has the issue of
messages running over several lines if a '\n' is present and destroying
the CSV file structure
"""
import os
from contextlib import contextmanager
import unicodecsv as csv


class CSVWriter(object):
    """ Abstract class to implement CSV writing """
    OUTPUT_FOLDER = 'output'

    def __init__(self, job_id, data_type):
        self.file_name = '{}_{}.csv'.format(
            job_id,
            data_type)
        self.path = '{}/{}/'.format(self.OUTPUT_FOLDER, job_id)
        try:
            os.mkdir(self.OUTPUT_FOLDER)
        except FileExistsError:  # Folder already created
            pass
        try:
            os.mkdir(self.path)
        except FileExistsError:  # Folder already created
            pass
        self.header()

    def header(self):
        """ Writes the header row for the CSV file """
        raise NotImplementedError

    def row(self, data):
        """ Writes the data row for the CSV file """
        raise NotImplementedError

    def write(self, line):
        """ Generic method to open and write a row to a CSV file """
        with self.open_csv(self.path + self.file_name) as writer:
            writer.writerow(line)

    @staticmethod
    @contextmanager
    def open_csv(path):
        """
        Makes sure that the files are opened and closed properly using the
        decorator pattern
        """
        the_file = open(path, 'ab')
        writer = csv.writer(the_file,
                            dialect='excel',
                            encoding='utf-8',
                            delimiter=',',
                            quotechar='"',
                            quoting=csv.QUOTE_NONNUMERIC)
        yield writer
        the_file.close()


class AttachmentWriter(CSVWriter):
    """Implementation of a class to write attachments """

    def __init__(self, job_id):
        super().__init__(job_id, 'attachments')

    def header(self):
        self.write((
            'to_id',  # Post or comment containing attachments
            'description',
            'description_tags',  # This is a list
            'media',
            'target',
            'title',
            'type',
            'url'
            ))

    def row(self, data):  # some might be empty!
        self.write((
            data['to_id'],  # must be added before
            data['description'] if 'description' in data
            else 'n/a',
            data['description_tags'] if 'description_tags' in data
            else 'n/a',
            data['media'] if 'media' in data else 'n/a',
            data['target'] if 'target' in data else 'n/a',
            data['title'] if 'title' in data else 'n/a',
            data['type'] if 'type' in data else 'n/a',
            data['url'] if 'url' in data else 'n/a'
            ))


class ReactionWriter(CSVWriter):
    """ Implementation of a class to write reactions """

    def __init__(self, job_id):
        super().__init__(job_id, 'reactions')

    def header(self):
        self.write((
            'to_id',
            'reaction_type',
            'user_id',
            'user_name'
            ))

    def row(self, data):
        self.write((
            data['to_id'],  # Must add to dictionary
            data['type'],
            data['id'],
            data['name']
            ))


class PostWriter(CSVWriter):
    """ Implementation of a class to write posts """

    def __init__(self, job_id):
        super().__init__(job_id, 'posts')

    def header(self):
        self.write((
            'id',
            'story',
            'created_time',
            'message',
            'picture',
            'caption',
            'description',
            'from_id',
            'from_name',
            'name',
            'object_id',
            'parent_id',
            'source',
            'status_type',
            'type',
            'updated_time',
            'share_count'
            ))

    # def row(self, data):
    #     self.write((
    #         data['id'],
    #         data['story'].encode('utf-8') if 'story' in data else 'n/a',
    #         data['created_time'],
    #         data['message'].encode('utf-8') if 'message' in data
    #         else 'n/a',
    #         data['picture'] if 'picture' in data else 'n/a',
    #         data['caption'].encode('utf-8') if 'caption' in data
    #         else 'n/a',
    #         data['description'].encode('utf-8') if 'description' in data
    #         else 'n/a',
    #         data['from']['id'],
    #         data['from']['name'].encode('utf-8'),
    #         data['name'].encode('utf-8') if 'name' in data else 'n/a',
    #         data['object_id'] if 'object_id' in data else 'n/a',
    #         data['parent_id'] if 'parent_id' in data else 'n/a',
    #         data['source'].encode('utf-8') if 'source' in data else 'n/a',
    #         data['status_type'] if 'status_type' in data else 'n/a',
    #         data['type'] if 'type' in data else 'n/a',
    #         data['updated_time'] if 'updated_time' in data else 'n/a'
    #         ))

    def row(self, data):
        self.write((
            data['id'],
            data['story'] if 'story' in data else 'n/a',
            data['created_time'],
            data['message'] if 'message' in data
            else 'n/a',
            data['picture'] if 'picture' in data else 'n/a',
            data['caption'] if 'caption' in data
            else 'n/a',
            data['description'] if 'description' in data
            else 'n/a',
            data['from']['id'],
            data['from']['name'],
            data['name'] if 'name' in data else 'n/a',
            data['object_id'] if 'object_id' in data else 'n/a',
            data['parent_id'] if 'parent_id' in data else 'n/a',
            data['source'] if 'source' in data else 'n/a',
            data['status_type'] if 'status_type' in data else 'n/a',
            data['type'] if 'type' in data else 'n/a',
            data['updated_time'] if 'updated_time' in data else 'n/a',
            data['shares']['count'] if 'shares' in data else '0'
            ))


class CommentWriter(CSVWriter):
    """
    Implementation of a class to write comments

    TODO:
    Fields:
        - attachment_url
        - attachment_id
        - source
    Options:
        - Order
        - Filter
    https://developers.facebook.com/docs/graph-api/reference/v2.9/object/comments
    """

    def __init__(self, job_id):
        super().__init__(job_id, 'comments')

    def header(self):
        self.write((
            'to_id',
            'message',
            'comment_id',
            'user_id',
            'user_name',
            'created_time',
            'like_count',
            'comment_count',
            'comm_type'
            ))

    def row(self, data):
        self.write((
            data['to_id'],  # Must add to dict
            data['message']  # Not utf-8 anymore
            if 'message' in data else 'n/a',  # Delete '\n' (should it?)
            data['id'],
            data['from']['id'],
            data['from']['name'],  # Removed utf-8 encoding
            data['created_time'],
            data['like_count'],
            data['comment_count'] if 'comment_count' in data else 'n/a',
            data['comm_type']  # Must add to dict
            ))


class SharedPostsWriter(CSVWriter):
    """
    Implementation of a class to write comments

    TODO:
        - Still unsure about the fields of this one
        - 'To' returns an array, I am treating it as an array
        with a single value and have not yet seen an exception
        but best keep this in mind
    """

    def __init__(self, job_id):
        super().__init__(job_id, 'sharedposts')

    def header(self):
        self.write((
            'from_id',
            'id',
            'story',
            'from_id',
            'from_name',
            'to_id',
            'to_name',
            'created_time',
            'updated_time'
            ))

    def row(self, data):
        self.write((
            data['to_id'],
            data['id'],
            data['story'] if 'story' in data else 'n/a',
            data['from']['id'],
            data['from']['name'],
            data['to']['data'][0]['id'] if 'to' in data else 'n/a',
            data['to']['data'][0]['name'] if 'to' in data else 'n/a',
            data['created_time'],
            data['updated_time']
            ))
