#!fbscrape/bin/python
# -*- coding: utf-8 -*-

import load_csv
from fb_gexf.fbgexf import CoReactionGraph, GexfManager, UserCoInteractionGraph

JOB_ID = ''  # The name of the folder (i.e. '2017-06-02_14_52_02_216796_group_feed_233739026689021')


def main():
    # args = parse_arguments()
    file = JOB_ID
    post_file = 'output/{}/{}'.format(file, file)
    posts = load_csv.load_posts('{}_posts.csv'.format(post_file))
    reactions = load_csv.load_reactions('{}_reactions.csv'.format(post_file))
    comments = load_csv.load_comments('{}_comments.csv'.format(post_file))

    mgr = GexfManager()

    # Use either/or. Comment for no-use
    mgr.add_method(CoReactionGraph(post_file))  # Very heavy one!
    mgr.add_method(UserCoInteractionGraph(post_file))

    mgr.load(posts, comments, reactions)
    mgr.write()

if __name__ == "__main__":
    main()