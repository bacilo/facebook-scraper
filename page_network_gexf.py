import pandas as pd
# import numpy as np
from fb_gexf.gexf import Gexf

JOB_ID = ''  # The name of the folder (i.e. '2017-06-18_14_02_15_592837_page_feed_BlackLivesMatter')


class PageNetworkGraph(object):
    def __init__(self):
        self.job_id = JOB_ID
        self.path = 'output/{}/{}'.format(self.job_id, self.job_id)
        self.posts = pd.read_csv('{}_posts.csv'.format(self.path))
        self.sharedposts = pd.read_csv('{}_sharedposts.csv'.format(self.path))
        self.nodes = dict()
        self.links = []

    def process_sharedposts(self):
        sp = self.sharedposts[['origin_id', 'from_id', 'to_id', 'from_name', 'to_name']].copy()
        sp.origin_id = sp.origin_id.str.extract('([0-9]+)', expand=True)
        p = self.posts[['id', 'from_id', 'from_name', 'parent_id', 'name']].copy()
        p.id = self.posts.id.str.extract('([0-9]+)', expand=True)
        for index, row in sp.iterrows():
            # import ipdb; ipdb.set_trace()
            self.add_node(row['origin_id'], p.loc[p['id'] == row['origin_id']].iloc[0]['from_name'])
            if row['to_id'] == 'n/a':
                self.add_node(row['from_id'], row['from_name'])
                self.add_link(row['origin_id'], row['from_id'])
            else:
                self.add_node(row['to_id'], row['to_name'])
                self.add_link(row['origin_id'], row['to_id'])

    def process_posts(self):
        p = self.posts[['id', 'from_id', 'from_name', 'parent_id', 'name']].copy()
        p.id = self.posts.id.str.extract('([0-9]+)', expand=True)
        p_done = p.ix[p['parent_id'] != 'n/a'].copy()
        p_done.parent_id = p.parent_id.str.extract('([0-9]+)', expand=True)
        for index, row in p_done.iterrows():
            self.add_node(row['id'], row['from_name'])
            self.add_node(row['parent_id'], row['name'])
            self.add_link(row['parent_id'], row['id'])

        sp = self.sharedposts[['origin_id', 'from_id', 'to_id', 'from_name', 'to_name']].copy()
        sp.origin_id = sp.origin_id.str.extract('([0-9]+)', expand=True)
        for index, row in sp.iterrows():
            self.add_node(row['origin_id'], p.loc[p['id'] == row['origin_id']].iloc[0]['from_name'])
            if row['to_id'] == 'n/a':
                self.add_node(row['from_id'], row['from_name'])
                self.add_link(row['origin_id'], row['from_id'])
            else:
                self.add_node(row['to_id'], row['to_name'])
                self.add_link(row['origin_id'], row['to_id'])

    def add_node(self, node, label):
        if node not in self.nodes:
            self.nodes[node] = dict()
            self.nodes[node]['id'] = str(node)
            self.nodes[node]['size'] = 5
            self.nodes[node]['label'] = label

    def add_link(self, source, target):
        self.links.append((source, target))

    def outputGEXF(self):
        gexf = Gexf("Creator Name", "Test Gexf File")
        graph = gexf.addGraph('directed', "static", "a test graph")
        output_file = 'fbscrape_' + 'page_graph' + '.gexf'

        nameAtt = graph.addNodeAttribute('name', '', 'string')

        for node in list(self.nodes.values()):
            r_n = graph.addNode(node['id'], node['label'])
            # r_n.addAttribute(nameAtt, post['comments']['summary']['total_count'])

        cnt = 0
        # for link in list(links.values()):
        for link in self.links:
            # r_e = graph.addEdge(str(cnt), link['source'], link['target'])
            r_e = graph.addEdge(str(cnt), str(link[0]), str(link[1]))
            cnt += 1

        gexf.write(open(output_file, 'w'))

pgng = PageNetworkGraph()
pgng.process_posts()
pgng.process_sharedposts()
pgng.outputGEXF()
