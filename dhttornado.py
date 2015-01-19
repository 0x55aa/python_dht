# coding:utf-8
import os
import ast

import dht
from dht import DHTPeer
import json

import tornado.ioloop

import tornado.options
from tornado.options import define, options
import tornado.web
import tornado.httpserver


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, DHTPeer):
            return (obj.id.encode("hex"), obj.ip_port[0], obj.ip_port[1])
        else:
            return json.JSONEncoder.default(self, obj)


class AHandler(tornado.web.RequestHandler):
    @classmethod
    def register_dht(self, dht):
        self.dht = dht

    def get(self):
        d = {}

        # for id, n in self.dht.node_lists.iteritems():
        #     d["Node_List_%s" % id.encode("hex")] = n.get_debug_array()
        self.set_header('Content-Type', 'text/plain')
        self.write(json.dumps(d, indent=2, cls=ComplexEncoder))


class BHandler(tornado.web.RequestHandler):
    @classmethod
    def register_dht(self, dht):
        self.dht = dht

    def get(self):
        d = {}
        for id in self.dht.infohash_peers:
            key = "Peers_for_%s" % id.encode("hex")
            d[key] = self.dht.infohash_peers[id].keys()

        self.set_header('Content-Type', 'text/plain')
        self.write(json.dumps(d, indent=2, cls=ComplexEncoder))


class DumpPeersHandler(tornado.web.RequestHandler):
    """
    备份所有的peers
    """
    @classmethod
    def register_dht(self, dht):
        self.dht = dht

    def get(self):
        d = {}
        for id in self.dht.infohash_peers:
            d[id.encode("hex")] = self.dht.infohash_peers[id].keys()
        f = open("/tmp/peers.txt", 'w')
        f.write(repr(d))
        f.close()
        self.write("success!!")


class DumpHandler(tornado.web.RequestHandler):
    """
    备份所有的ip:port
    """
    @classmethod
    def register_dht(self, dht):
        self.dht = dht

    def get(self):
        def populate(tree, l):
            if tree.left:
                populate(tree.left, l)
            if tree.right:
                populate(tree.right, l)
            if tree.value:
                for i in tree.value:
                    l.append(tuple(i.ip_port))

            return l

        rt = []

        populate(self.dht.routing_table._root, rt)

        f = open("/tmp/ip_port.txt", 'w')
        f.write(repr(rt))
        f.close()
        self.write("success!!")


class DumpMemHandler(tornado.web.RequestHandler):
    """
    导出内存
    """
    @classmethod
    def register_dht(self, dht):
        self.dht = dht

    def get(self):
        from meliae import scanner
        import time
        scanner.dump_all_objects('/tmp/dump%s.txt' % time.time())
        self.write("success!!")


class IndexHandler(tornado.web.RequestHandler):
    @classmethod
    def register_dht(self, dht):
        self.dht = dht

    def get(self):

        def populate(tree, dict):
            if tree.left:
                dict['left'] = {}
                populate(tree.left, dict['left'])
            if tree.right:
                dict['right'] = {}
                populate(tree.right, dict['right'])
            if tree.value:
                dict['value'] = tree.value

            return dict

        rt = {}

        populate(self.dht.routing_table._root, rt)

        d = {
            # 'zid': str(self.dht.id),
            # 'aid': str(self.dht.id),
            'dht': str(self.dht),
            'routing_table': str(self.dht.routing_table),
            'rt': rt
        }

        self.set_header('Content-Type', 'text/plain')
        self.write(json.dumps(d, indent=2, cls=ComplexEncoder))


if __name__ == "__main__":
    ioloop = tornado.ioloop.IOLoop()
    ioloop.install()

    define('debug', default=True, type=bool)  # save file causes autoreload
    define('frontend_port', default=7070, type=int)

    tornado.options.parse_command_line()
    # settings = dict((k, v.value()) for k, v in options.items())
    settings = {}

    ip_ports = [('router.bittorrent.com', 6881),
                ('router.bitcomet.com', 6881),
                ('router.utorrent.com', 6881),
                ('dht.transmissionbt.com', 6881),
                ("service.ygrek.org.ua", 6881),
                ("router.transmission.com", 6881),
                ]
    # 读dump
    path = "/tmp/ip_port.txt"
    if os.path.isfile(path):
        f = open(path)
        l = ast.literal_eval(f.read())
        ip_ports = ip_ports + l

    # print(ip_ports)

    # 23649f6ace4b4062879066a6afe99b91c1880b8f
    node_id = 'd54408eb2a5d686bd3e587f7a96c2facebbeadfc'.decode('hex')
    dht = dht.DHT(51414, ip_ports, node_id=node_id)

    frontend_routes = [
        ('/c', IndexHandler),
        ('/a', AHandler),
        ('/b', BHandler),
        ('/dump', DumpHandler),
        ('/dumppeers', DumpPeersHandler),
        ('/dumpmem', DumpMemHandler),
    ]
    frontend_application = tornado.web.Application(frontend_routes, **settings)
    frontend_server = tornado.httpserver.HTTPServer(frontend_application,
                                                    io_loop=ioloop)
    frontend_server.bind(options.frontend_port, '')
    frontend_server.start()

    IndexHandler.register_dht(dht)
    AHandler.register_dht(dht)
    BHandler.register_dht(dht)
    DumpHandler.register_dht(dht)
    DumpMemHandler.register_dht(dht)
    DumpPeersHandler.register_dht(dht)

    dht.bootstrap()
    dht.start()
