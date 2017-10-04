#!fbscrape/bin/python
# -*- coding: utf-8 -*-

import unicodecsv as csv

# self.writer[name] = csv.writer(self.files[name], dialect='excel', encoding='utf-8', delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

def load_posts(filename):
    f = open(filename, 'rb')
    r = csv.reader(f, encoding='utf-8')
    # import ipdb; ipdb.set_trace()
    posts = []
    next(r)
    for row in r:
        posts.append(add_post(row))
    return posts

def load_reactions(filename):
    f = open(filename, 'rb')
    r = csv.reader(f, encoding='utf-8')
    reactions = []
    next(r)
    for row in r:
        reactions.append(add_reaction(row))
    return reactions

def load_comments(filename):
    f = open(filename, 'rb')
    r = csv.reader(f, encoding='utf-8')
    comments = []
    next(r)
    for row in r:
        comments.append(add_comment(row))
    return comments

def add_reaction(row):
    reaction = dict()
    reaction['post_id'] = row[0]
    reaction['type'] = row[1]
    reaction['id'] = row[2]
    reaction['name'] = row[3]
    return reaction

def add_comment(row):
    comment = dict()
    comment['post_id'] = row[0]
    comment['message'] = row[1]
    comment['comment_id'] = row[2]
    comment['user_id'] = row[3]
    comment['name'] = row[4]
    return comment

def add_post(row):
    post = dict()
    post['from'] = dict()
    post['reactions'] = dict()
    post['reactions']['summary'] = dict()
    post['shares'] = dict()
    post['comments'] = dict()
    post['comments']['summary'] = dict()

    post['id'] = row[0]
    post['story'] = row[1]
    post['created_time'] = row[2]
    post['description'] = row[6]
    post['from']['id'] = row[7]
    post['from']['name'] = row[8]


    # post['shares']['count'] = row[12]
    # post['reactions']['summary']['total_count'] = row[13]
    # post['reactions']['summary']['viewer_reaction'] = row[14]
    # post['comments']['summary']['total_count'] = row[15]
    # post['comments']['summary']['can_comment'] = row[16]
    # post['comments']['summary']['order'] = row[17]
    # post['is_instagram_eligible'] = row[18]
    # #This next one is probably contingent on type (as well as others I guess)
    # post['link'] = row[19]
    # post['permalink_url'] = row[20]
    # post['icon'] = row[21]
    # post['name'] = row[22]
    # post['updated_time'] = row[23]
    # post['caption'] = row[24]
    # post['is_published'] = row[25]
    # Check for error message 'ineligible_unknown_error'
    # post['instagram_eligibility'] = row[26]
    # post['privacy']['allow'],
    # post['privacy']['deny'],
    # post['privacy']['friends'],
    # post['privacy']['description'],
    # post['privacy']['value'],

    return post