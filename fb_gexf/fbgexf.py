#!fbscrape/bin/python
# -*- coding: utf-8 -*-

from .gexf import Gexf
import itertools


class GexfManager():
    def __init__(self):
        self.output_methods = []

    def add_method(self, method):
        self.output_methods.append(method)

    def add_post(self, post):
        for method in self.output_methods:
            method.add_post(post)

    def add_reaction(self, reaction):
        for method in self.output_methods:
            method.add_reaction(reaction)

    def add_comment(self, reaction):
        for method in self.output_methods:
            method.add_comment(reaction)

    def analysis(self):
        for method in self.output_methods:
            method.analysis()

    def load(self, posts, comments, reactions):
        for method in self.output_methods:
            method.load(posts, comments, reactions)

    def write(self):
        for method in self.output_methods:
            method.write()


class OutputGEXF():

    def __init__(self, description, g_type):
        self.gexf = Gexf("Pedro Ferreira", "Test Gexf File")
        self.graph = self.gexf.addGraph(g_type, "static", "a test graph")
        self.output_file = description + '.gexf'

    def add_post(self, post):
        raise NotImplementedError("Should have implemented 'add_post' method")

    def add_reaction(self, reaction, post_id):
        pass

    def add_comment(self, comment, post_id):
        pass

    def analysis(self):
        pass

    def load(self, posts, comments, reactions):
        pass

    def write(self):
        self.gexf.write(open(self.output_file, 'w'))


class CoReactionGraph(OutputGEXF):
    """
    The idea is to generate a graph where:

    Nodes: posts
    Edges: there is a connection between two nodes, if a certain user has the
        same reaction to both posts (i.e. 'LOVE's them both or comments both
        for instance)
    """

    def __init__(self, sufix):
        OutputGEXF.__init__(self, sufix + '_CoReaction', 'undirected')
        self.nbr_comments = self.graph.addNodeAttribute(
            'nbrComment', '0', 'integer')
        self.nbr_reactions = self.graph.addNodeAttribute(
            'nbrReactions', '0', 'integer')
        self.post_msg = self.graph.addNodeAttribute(
            'message', '', 'string')
        self.user_name = self.graph.addNodeAttribute(
            'user_name', '', 'string')
        self.user_id = self.graph.addNodeAttribute(
            'user_id', '', 'string')

        # Probably best as a list[] than as a post as it will be used only
        # once in linear fashion (but harder to add...)
        self.react_by_user = dict()
        self.comment_by_user = dict()
        self.comments_by_post = dict()  # This should have been scraped already
        self.reactions_by_post = dict()  # This should have been scraped as well

    def load(self, posts, comments, reactions):
        print('adding reactions')
        for reaction in reactions:
            self.add_reaction(reaction)
        print('adding comments')
        for comment in comments:
            self.add_comment(comment)
        print('adding posts')
        for post in posts:
            self.add_post(post)
        print('performing analysis... this one might take a while for large datasets')
        self.analysis()

    def add_post(self, post):
        r_n = self.graph.addNode(
            post['id'], post['id'])
        if post['id'] not in self.comments_by_post:
            self.comments_by_post[post['id']] = 1
        r_n.addAttribute(
            self.nbr_comments, str(self.comments_by_post[post['id']]))
        if post['id'] not in self.reactions_by_post:
            self.reactions_by_post[post['id']] = 1
        r_n.addAttribute(
            self.nbr_reactions, str(self.reactions_by_post[post['id']]))
        r_n.addAttribute(
            self.post_msg, post['description'].encode("ascii", errors='replace'))
        r_n.addAttribute(
            self.user_name, post['from']['name'].encode("ascii", errors='replace'))
        r_n.addAttribute(
            self.user_id, post['from']['id'])

    def add_reaction(self, reaction):

        if reaction['post_id'] not in self.reactions_by_post:
            self.reactions_by_post[reaction['post_id']] = 1
        else:
            self.reactions_by_post[reaction['post_id']] += 1

        if reaction['id'] in self.react_by_user:
            if reaction['type'] in self.react_by_user[reaction['id']]:
                self.react_by_user[reaction['id']][reaction['type']].append(
                    reaction['post_id'])
            else:
                self.react_by_user[reaction['id']][reaction['type']] = []
                self.react_by_user[reaction['id']][reaction['type']].append(
                    reaction['post_id'])
        else:
            self.react_by_user[reaction['id']] = dict()
            self.react_by_user[reaction['id']][reaction['type']] = []
            self.react_by_user[reaction['id']][reaction['type']].append(
                reaction['post_id'])

    def add_comment(self, comment):
        
        if comment['post_id'] not in self.comments_by_post:
            self.comments_by_post[comment['post_id']] = 1
        else:
            self.comments_by_post[comment['post_id']] += 1

        if comment['user_id'] in self.comment_by_user:
            self.comment_by_user[comment['user_id']].append(comment['post_id'])
        else:
            self.comment_by_user[comment['user_id']] = []
            self.comment_by_user[comment['user_id']].append(comment['post_id'])

    def analysis(self):
        cnt = 0

        cmnt_cnt = 0
        total_cmtn_cnt = len(self.comment_by_user.items())
        reac_cnt = 0
        total_reac_cnt = len(self.react_by_user.items())
        # Comment part still untested
        for x in self.comment_by_user.items():
            cmnt_cnt += 1
            print('processed {} of {} comments            '.format(cmnt_cnt, total_cmtn_cnt))
            for c in itertools.combinations(x[1], 2):
                try:
                    r_e = self.graph.addEdge(str(cnt), c[0], c[1])
                except Exception:
                    pass
                # r_e = self.graph.addEdge(str(cnt), c[0], c[1])
                cnt = cnt + 1

        for x in self.react_by_user.items():
            reac_cnt += 1
            print('processed {} of {} reactions               '.format(reac_cnt, total_reac_cnt))
            for y in x[1].items():
                if cnt % 100 == 0:
                    print(str(cnt) + ' - ' + str(y[1]))
                # print 'add_edge for ' + str(k1) + str(cnt)
                # At this point we are dealing with a list of same reactions by the same user
                for c in itertools.combinations(y[1], 2):
                    try:
                        r_e = self.graph.addEdge(str(cnt), c[0], c[1])
                    except Exception:
                        pass
                    # r_e = self.graph.addEdge(str(cnt), c[0], c[1])

                    cnt = cnt + 1


class UserCoInteractionGraph(OutputGEXF):
    """ 
    The idea is to generate a graph where:

    Nodes: users that either posted, commented or reacted
    Edges: Two nodes (users) are connected if one has produced content
        (i.e. post or comment), and the other has reacted to that
        (i.e. cmmented or reacted to)
    """

    def __init__(self, sufix):
        OutputGEXF.__init__(self, sufix + '_UserCoInteraction', 'directed')
        # self.commAtt = self.graph.addNodeAttribute('nbrComment', '0', 'integer')
        # self.reactAtt = self.graph.addNodeAttribute('nbrReactions', '0', 'integer')
        self.react_by_user = dict()
        self.nameAtt = self.graph.addNodeAttribute('name', '', 'string')
        self.element = dict()
        self.cnt = 0

    def add_post(self, post):
        r_n = self.graph.addNode(post['from']['id'], post['from']['name'].encode("ascii", errors='replace'))
        # r_n.addAttribute(self.nameAtt, post['comments']['summary']['total_count'])
        self.element[post['id']] = post['from']['id']

    def add_reaction(self, reaction):
        r_n = self.graph.addNode(reaction['id'], reaction['name'].encode("ascii", errors='replace'))
        r_e = self.graph.addEdge(str(self.cnt), reaction['id'], self.element[reaction['post_id']])
        self.cnt = self.cnt + 1

    # Comments MUST be added before reactions as they are also elements
    def add_comment(self, comment):
        """ 
        Comments are both: (1) elements that users can have a common
        reaction  to, and (2) a 'reaction' that can be common to two users
        """
        r_n = self.graph.addNode(comment['user_id'], comment['name'].encode("ascii", errors='replace'))
        self.element[comment['comment_id']] = comment['user_id']
        r_e = self.graph.addEdge(str(self.cnt), comment['user_id'], self.element[comment['post_id']])
        self.cnt = self.cnt + 1

    def load(self, posts, comments, reactions):
        print('adding posts (total: {})'.format(len(posts)))
        for post in posts:
            self.add_post(post)
        print('adding comments (total: {})'.format(len(comments)))
        for comment in comments:
            self.add_comment(comment)
        print('adding reactions (total: {})'.format(len(reactions)))
        for reaction in reactions:
            self.add_reaction(reaction)

