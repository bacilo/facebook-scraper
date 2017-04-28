# -*- coding: utf-8 -*-
"""CSV Writer

This class will be dumping the scraped data to different CSV files
"""

import os
import datetime
import logging
from contextlib import contextmanager
import unicodecsv as csv


class CSVWriter(object):
    """
    This class is responsible for writing the data to different CSV files
    """
    OBJECT_TYPES = ['posts', 'reactions', 'comments', 'story_tags']
    # 'sharedposts', 'insights', 'attachments', 'message_tags', 'properties',
    # 'to', 'with_tags']
    OUTPUT_FOLDER = 'output/'

    file_names = {}

    def __init__(self, scrape_id):
        """
        Initializes the filenames to be used for this particular scraping task
        Uses timestamp to mark the files and make them identifiable as
        belonging together (e.g. 'reactions' and 'posts' will appear on
        different files with the same timestamp)

        It starts by cleaning up the output folder so make sure you have saved
        previous scrapes
        """
        self.restart_output_folder()
        timestamp = str(datetime.datetime.utcnow()).replace(':', '_') \
            .replace('.', '_')
        sub_folder = '{}{}_{}/'.format(
            self.OUTPUT_FOLDER, scrape_id, timestamp)
        os.mkdir(sub_folder)
        for obj_type in self.OBJECT_TYPES:
            self.file_names[obj_type] = '{}{}_{}_{}.csv'.format(
                sub_folder, scrape_id, timestamp, obj_type)
        self.add_post_header()
        self.add_reaction_header()
        self.add_comment_header()

    def add_post_header(self):
        """
        Writes the first line (header) to the CSV file for posts
        """
        with self.open_csv(self.file_names['posts']) as writer:
            writer.writerow((
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
                'updated_time'
                ))

    def add_post(self, post):
        """
        Given a post structure, it writes the corresponding line to the
        CSV file
        """
        with self.open_csv(self.file_names['posts']) as writer:
            writer.writerow((
                post['id'],
                post['story'].encode('utf-8') if 'story' in post else 'n/a',
                post['created_time'],
                post['message'].encode('utf-8') if 'message' in post
                else 'n/a',
                post['picture'] if 'picture' in post else 'n/a',
                post['caption'].encode('utf-8') if 'caption' in post
                else 'n/a',
                post['description'].encode('utf-8') if 'description' in post
                else 'n/a',
                post['from']['id'],
                post['from']['name'].encode('utf-8'),
                post['name'].encode('utf-8') if 'name' in post else 'n/a',
                post['object_id'] if 'object_id' in post else 'n/a',
                post['parent_id'] if 'parent_id' in post else 'n/a',
                post['source'].encode('utf-8') if 'source' in post else 'n/a',
                post['status_type'] if 'status_type' in post else 'n/a',
                post['type'] if 'type' in post else 'n/a',
                post['updated_time'] if 'updated_time' in post else 'n/a'
                ))

# FEED_FIELDS = (
#     "message_tags,"
#     "shares,with_tags"
#     )

    def add_reaction_header(self):
        """
        Writes the first line (header) to the CSV file for reactions
        """
        with self.open_csv(self.file_names['reactions']) as writer:
            writer.writerow((
                'post_id',
                'reaction',
                'user_id',
                'user_name'
                ))

    def add_reaction(self, reaction, post_id):
        """
        Given a reaction structure, it writes the corresponding line
        to the CSV file
        """
        with self.open_csv(self.file_names['reactions']) as writer:
            writer.writerow((
                post_id,
                reaction['type'],
                reaction['id'],
                reaction['name'].encode('utf-8')
                ))

    def add_comment_header(self):
        """
        Writes the first line (header) to the CSV file for comments
        """
        with self.open_csv(self.file_names['comments']) as writer:
            writer.writerow((
                'post_id',
                'message',
                'comment_id',
                'user_id',
                'user_name',
                'created_time',
                'like_count',
                'comment_count',
                'sub_comment'
                ))

    def add_comment(self, comment, post_id, sub_comment):
        """
        Given a comment structure, it writes the corresponding line to the
        CSV file
        """
        with self.open_csv(self.file_names['comments']) as writer:
            writer.writerow((
                post_id,
                comment['message'].replace('\n', ' ')  # Not utf-8 anymore
                if 'message' in comment else 'n/a',  # Delete '\n' (should it?)
                comment['id'],
                comment['from']['id'],
                comment['from']['name'],  # Removed utf-8 encoding
                comment['created_time'],
                comment['like_count'],
                comment['comment_count']
                if 'comment_count' in comment else 'n/a',
                '1' if sub_comment else '0'
                ))

    def restart_output_folder(self):
        """
        This method deletes all the files in the OUTPUT_FOLDER
        """
        try:
            for the_file in os.listdir(self.OUTPUT_FOLDER):
                file_path = os.path.join(self.OUTPUT_FOLDER, the_file)
        except FileNotFoundError:
            logging.debug('output folder not existing, creating!')
            os.mkdir(self.OUTPUT_FOLDER)
            return

        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            # elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as exc:
            logging.error(exc)

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
