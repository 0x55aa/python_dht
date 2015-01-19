# coding:utf-8
import os
import ast
import random
from bencode import bdecode, bencode
import hashlib
import socket
import time
from struct import unpack, pack
import logging

import tornado.ioloop

import tornado.options
import tornado.web

from peer import DHTPeer, dht_tree, get_peers_callbacks, infohash_peers

URL = "http://127.0.0.1:9999/web/insert/"
URL2 = "http://127.0.0.1:9999/web/insert2/"


# 添加infohash到数据库
def insert_into_database(info_hash, insert_url):
    # 这里直接请求web接口，让web添加
    import urllib2
    url = insert_url + info_hash.encode("hex") + '/'
    try:
        res = urllib2.urlopen(url).read()
        if not res:
            logging.info("insert into database faild. hash: %s" % info_hash.encode('hex'))
    except:
        logging.info("insert into database faild! hash: %s" % info_hash.encode('hex'))


# This just returns a random number between 0 and MAX 32 Bit Int
def gen_unsigned_32bit_id():
    return random.randint(0, 0xFFFFFFFF)


# This just returns a random unsigned int
def gen_signed_32bit_id():
    return random.randint(-2147483648, 2147483647)


# Generates a string of 20 random bytes
def gen_peer_id():
    return gen_random_string_of_bytes(20)


def gen_random_string_of_bytes(length):
    return ''.join(chr(random.randint(0, 255)) for x in range(length))


def array_to_string(arr):
    return ''.join(chr(x) for x in arr)


def xor_array(n1, n2):
    return [ord(n1[i]) ^ ord(n2[i]) for i, _ in enumerate(n1)]


def dht_dist(n1, n2):
    return xor_array(n1, n2)


class DHTQuery(object):
    """
    保存发送的消息，用于判断消息的合法
    将直接保存msg修改成单个对象了
    """
    __slots__ = ('time_sent', 't_id', 'a_target', 'a_info_hash', 'q')

    def __init__(self, t_id=None, a_target=None, a_info_hash=None, q=None):
        self.time_sent = time.time()
        #
        self.t_id = t_id
        self.a_target = a_target
        self.a_info_hash = a_info_hash
        self.q = q

    def free(self):
        self.time_sent = None
        #
        self.t_id = None
        self.a_target = None
        self.a_info_hash = None
        self.q = None


class DHT(object):
    NODE_ID_IP_PORT_LENGTH = 26
    PONG_TIMEOUT = 5
    BOOTSTRAP_DELAY = 60
    TRANSACTION_ID_TIMEOUT = 30

    def __init__(self, port, bootstrap_ip_ports, node_id=None, io_loop=None):
        self.transaction_id = 0
        self.ip_ports = bootstrap_ip_ports

        self.port = port
        self.io_loop = io_loop or tornado.ioloop.IOLoop.instance()

        if not node_id:
            self.id = gen_peer_id()
        else:
            self.id = node_id

        logging.info(self.id.encode('hex'))
        # dht_tree 共用
        self.routing_table = dht_tree

        # 共用
        self.queries = {}  # queries

        # 下面两个变量共用
        self.get_peers_callbacks = get_peers_callbacks
        self.infohash_peers = infohash_peers

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.io_loop.add_handler(self.sock.fileno(), self.handle_input, self.io_loop.READ)

        # 用来查找node，保持活跃
        self.find_self = False

        random.seed()

    def generate_token(self):
        return str(self.id) + str(random.randint(0, 10000))

    def bootstrap_by_finding_myself(self):
        """
        检测所以节点,查找节点
        快速加入到网络中
        """
        if self.find_self:
            target = gen_peer_id()
        else:
            target = self.id
            self.find_self = True
        logging.info("Bootstrapping to %s" % target.encode("hex"))

        try:
            self.find_node(target)
            self.get_peers(target)
        except Exception, e:
            logging.error(str(e))
            self.io_loop.add_timeout(time.time() + DHT.BOOTSTRAP_DELAY + 10, self.bootstrap_by_finding_myself)

        self.io_loop.add_timeout(time.time() + DHT.BOOTSTRAP_DELAY, self.bootstrap_by_finding_myself)

    def get_trasaction_id(self):
        """
        生成t，kkkkk
        用于区分
        """
        self.transaction_id += 1
        # logging.info("%s,%s\n" % (self.transaction_id, pack("H", self.transaction_id)))

        if self.transaction_id >= 65534:
            self.transaction_id = 0

        return pack("H", self.transaction_id)

        # MESSAGES define at http://www.bittorrent.org/beps/bep_0005.html

        # MESSAGE: PING
        # ping Query = {"t":"aa", "y":"q", "q":"ping", "a":{"id":"abcdefg"}}
        # Response = {"t":"aa", "y":"r", "r": {"id":"mnopqrstuvwxyz123456"}}
    def ping(self, ip_port):
        """
        ping操作，好像只是用来区分本地操作的
        之后发送包
        只用用t的值做key，保存发送的包和ip.端口
        """
        # logging.info( "PING ----%s--->\n" % str(ip_port) )
        t_id = self.get_trasaction_id()
        # print t_id
        ping_msg = {"t": t_id, "y": "q", "q": "ping", "a": {"id": self.id}}
        self.sock.sendto(bencode(ping_msg), ip_port)
        self.queries[t_id] = DHTQuery(t_id=ping_msg['t'], q=ping_msg['q'])

    def got_ping_response(self, response, ip_port):
        """
        得到ping返回的包，从保存的信息里面取出ip.端口,然后就删除
        key为返回信息的t，根据协议就是你发送的t。
        ping通了就添加到 routing_table中,根据协议 id为ip的hash，
        """
        transaction_id = response["t"]

        self.queries[transaction_id].free()
        del self.queries[transaction_id]
        # logging.info("<----%s--- PONG\n" % str(q.ip_port))
        # logging.info("<%s----%s--- PONG\n" % (self.id.encode('hex'),id(self.routing_table)))
        self.routing_table.insert(DHTPeer(response['r']['id'], ip_port), self)

    def got_ping_query(self, query, source_ip_port):
        """
        处理别人发来的ping请求
        因为他发请求来，所以直接作为活的节点处理了，插入到routing_table
        接着发送自己的ip hash
        """
        # logging.info("< PING ..%s.. PONG > \n" % str(source_ip_port))
        self.routing_table.insert(DHTPeer(query['a']['id'], source_ip_port), self)
        pong_msg_reply = {"t": query["t"], "y": "r", "r": {"id": self.id}}
        self.sock.sendto(bencode(pong_msg_reply), source_ip_port)

    # MESSAGE: find_node
    # find_node Query = {"t":"aa", "y":"q", "q":"find_node", "a": {"id":"abcdefghij0123456789", "target":"mnopqrstuvwxyz123456"}}
    # Response = {"t":"aa", "y":"r", "r": {"id":"0123456789abcdefghij", "nodes": "def456..."}}
    def find_node(self, target):
        """
        查找指定node，对节点发送查找消息j
        """
        # Find the bucket in the routing table for the target
        closest_bucket = self.routing_table.get_target_bucket(target)

        if not closest_bucket:
            logging.info("There are no nodes in the routing table to send find node messages.")

        # logging.info("<send find node----%s--- FIND_NODES " % target.encode("hex"))
        # Iterate over and find closest N nodes
        for n in closest_bucket:
            # print n.ip_port
            self.send_find_node_message(target, n.ip_port)

    def send_find_node_message(self, target, ip_port):
        """
        发送查找消息
        """
        # logging.info("FIND NODE ----%s--->\n" % str(ip_port))
        t_id = self.get_trasaction_id()
        find_node_msg = {"t": t_id, "y": "q", "q": "find_node",
                         "a": {"id": self.id, "target": target}}

        try:
            self.sock.sendto(bencode(find_node_msg), ip_port)
        except:
            # import traceback
            # traceback.print_exc()
            # logging.info("except in send_find_node_message %s %s>\n" % \
            #        (str(ip_port), repr(find_node_msg)))
            pass
        self.queries[t_id] = DHTQuery(t_id=find_node_msg['t'],
                                      a_target=find_node_msg['a']['target'],
                                      q=find_node_msg['q'])

    def got_find_node_query(self, query, source_ip_port):
        """
        处理别人发来的查找请求
        """
        # logging.info("GET PEERS RESPONSE ----%s--->\n" % str(source_ip_port))

        # token = hashlib.sha1(self.generate_token()).digest()
        info_hash = query['a']['target']

        nodes = ""
        closest_bucket = self.routing_table.get_target_bucket(info_hash)
        for n in closest_bucket:
            ip_array = map(int, n.ip_port[0].split("."))
            cur_node_str = pack(">20sBBBBH", n.id, ip_array[0], ip_array[1], ip_array[2], ip_array[3], n.ip_port[1])
            nodes += cur_node_str

        find_node_response_msg = {"t": query["t"], "y": "r", "r": {"id": self.id, "nodes": nodes}}
        self.sock.sendto(bencode(find_node_response_msg), source_ip_port)

    def got_find_node_response(self, response, source_ip_port):
        """
        查找请求发送后，返回信息
        """
        # print "Got find_node response"
        transaction_id = response["t"]
        # logging.info("<----%s--- FIND_NODES " % target_id.encode("hex"))
        target_id = self.get_original_target_id_from_response(response)
        self.queries[transaction_id].free()
        del self.queries[transaction_id]

        if 'nodes' in response['r']:
            # print "The response has nodes"
            # 返回response里的节点
            l = self.add_nodes_to_heap(response, target_id)
            for n in l:
                self.send_find_node_message(target_id, n.ip_port)

        # else:
        #     logging.info("Response for find_node has no nodes:\n%s, %s" % (str(response), str(source_ip_port)))

    def get_original_target_id_from_response(self, response):
        """
        根据返回信息的t，获取查找的id
        """
        transaction_id = response["t"]
        return self.queries[transaction_id].a_target

    def get_original_info_hash_from_response(self, response):
        """
        根据返回信息的t，获取info_hash
        """
        transaction_id = response["t"]
        return self.queries[transaction_id].a_info_hash

    def add_nodes_to_heap(self, response, target_id):
        """
        添加查找node时 返回的node list
        """
        #logging.info("<%s-------%s--%s PONG\n" % (self.id.encode('hex'), id(self.get_peers_callbacks), id(self.get_peers_callbacks)))
        nodes_and_ip_port_str = response['r']['nodes']
        number_of_nodes = len(nodes_and_ip_port_str)/DHT.NODE_ID_IP_PORT_LENGTH

        l = []
        for cur_node in range(0, number_of_nodes):
            base_str_index = cur_node * DHT.NODE_ID_IP_PORT_LENGTH
            cur_node_id_ip_port_str = nodes_and_ip_port_str[base_str_index:base_str_index + DHT.NODE_ID_IP_PORT_LENGTH]
            id_bytes, ip_bytes, port = unpack(">20s4sH",  cur_node_id_ip_port_str)
            ip_str = '.'.join(map(str, map(ord, ip_bytes)))
            if ip_bytes == 0:
                logging.info("<------" * 10)
            #print "Adding node:%s (%s:%s)" % (id_bytes, ip_str, port)
            new_node = DHTPeer(id_bytes, (ip_str, port))
            self.routing_table.insert(new_node, self)
            #logging.info("<%s-len:--%s---" % (self.id.encode('hex'), str(self.routing_table.lenght)))
            l.append(new_node)

            #TODO Check if new node is the one im looking for?!
        return l

    """
    # MESSAGE: get_peers
    # get_peers Query = {"t":"aa", "y":"q", "q":"get_peers",
    #   "a": {"id":"abcdefghij0123456789", "info_hash":"mnopqrstuvwxyz123456"}}
    # Response with peers = {"t":"aa", "y":"r",
    #   "r": {"id":"abcdefghij0123456789",
              "token":"aoeusnth", "values": ["axje.u", "idhtnm"]}}
    # Response with closest nodes = {"t":"aa", "y":"r",
        "r": {"id":"abcdefghij0123456789",
              "token":"aoeusnth", "nodes": "def456..."}}
    """
    def get_peers(self, info_hash, callback=None):
        """
        获取peers
        """
        if callback:
            # info_hash_hex = info_hash.encode("hex")
            if not info_hash in self.get_peers_callbacks:
                self.get_peers_callbacks[info_hash] = []
            self.get_peers_callbacks[info_hash].append(callback)

        # Find the bucket in the routing table for the target
        closest_bucket = self.routing_table.get_target_bucket(info_hash)

        if not closest_bucket:
            logging.info("There are no nodes in the routing table to send get peers messages.")

        # logging.info("<send get peers----%s---  " % info_hash.encode("hex"))
        for n in closest_bucket:
            self.send_get_peers_message(info_hash, n.ip_port)

    def send_get_peers_message(self, info_hash, ip_port):
        """
        发送获取peer的消息
        """
        # logging.info("GET PEERS ----%s--->\n" % str(ip_port))
        trasaction_id = self.get_trasaction_id()
        get_peers_msg = {"t": trasaction_id, "y": "q", "q": "get_peers",
                         "a": {"id": self.id, "info_hash": info_hash}}

        try:
            self.sock.sendto(bencode(get_peers_msg), ip_port)
        except:
            # import traceback
            # traceback.print_exc()
            # logging.info("except in send_find_node_message %s %s>\n" % \
            #        (str(ip_port), repr(get_peers_msg)))
            pass

        self.queries[trasaction_id] = DHTQuery(
            t_id=get_peers_msg['t'],
            a_info_hash=get_peers_msg['a']['info_hash'],
            q=get_peers_msg['q'])

    def got_get_peers_query(self, query, source_ip_port):
        """
        别人请求消息，查找
        """
        # logging.info("GET PEERS RESPONSE ----%s--->\n" % str(source_ip_port))

        transaction_id = query["t"]
        token = hashlib.sha1(self.generate_token()).digest()
        info_hash = query['a']['info_hash']

        #添加infohash到数据库###
        #insert_into_database(info_hash, URL2)

        get_peers_response_dict = {"id": self.id, "token": token}

        if info_hash in self.infohash_peers:
            ip_ports = self.infohash_peers[info_hash].keys()
            values = []
            for ip_port in ip_ports:
                ip_array = map(int, ip_port[0].split("."))
                values.append(pack(">BBBBH", ip_array[0], ip_array[1], ip_array[2], ip_array[3], ip_port[1]))
            get_peers_response_dict['values'] = values
        else:
            nodes = ""
            closest_bucket = self.routing_table.get_target_bucket(info_hash)
            for n in closest_bucket:
                ip_array = map(int, n.ip_port[0].split("."))
                cur_node_str = pack(">20sBBBBH", n.id, ip_array[0], ip_array[1], ip_array[2], ip_array[3], n.ip_port[1])
                nodes += cur_node_str

            get_peers_response_dict['nodes'] = nodes

        get_peers_response_msg = {"t": transaction_id, "y": "r", "r": get_peers_response_dict}
        self.sock.sendto(bencode(get_peers_response_msg), source_ip_port)

    def got_get_peers_response(self, response, source_ip_port):
        """
        请求后，处理返回的消息
        """
        # import pdb; pdb.set_trace()
        # print "Got get_peers response"

        transaction_id = response["t"]
        target_id = self.get_original_info_hash_from_response(response)

        #logging.info("<----%s--- GET_PEERS \n" % target_id.encode("hex"))

        #import pdb; pdb.set_trace()
        if 'nodes' in response['r']:
            #logging.info("Got nodes")
            #print "The get_peers response has nodes"
            l = self.add_nodes_to_heap(response, target_id)
            for n in l:
                self.send_get_peers_message(target_id, n.ip_port)

        elif 'values' in response['r']:
            #print "\n\n!!!!The get_peers has values!!!!\n\n"
            self.add_peers_to_list(response, target_id)
            #logging.info( str(self.infohash_peers[self.infohash_peers.keys()[0]].keys()) )
            #import pdb; pdb.set_trace()
            #XXX: Maybe I should announce back even if they dont have a peer list for me?

            """
            if target_id in self.queries:
                info_hash = slef.queries[target_id].msg['a']['info_hash']
                if info_hash in self.infohash_peers:
                    ip_ports = self.infohash_peers[info_hash].keys()
                    for ip_port in ip_ports:
                        self.announce_peer(target_id, ip_port, response)
            """
            #info_hash = self.queries[transaction_id].msg['a']['info_hash']
            info_hash = self.get_original_info_hash_from_response(response)
            ip_ports = self.infohash_peers[info_hash].keys()
            for ip_port in ip_ports:
                self.announce_peer(target_id, ip_port, response)
            logging.info("<send----%s--- ANNOUNCE_PEER \n" % info_hash.encode("hex"))
            #添加infohash到数据库###
            insert_into_database(info_hash, URL)
        else:
            logging.info("Response for find_node has no nodes:\n%s, %s" % (str(response), str(source_ip_port)))

        self.queries[transaction_id].free()
        del self.queries[transaction_id]
        #print str(self.routing_table._root)

    def add_peers_to_list(self, response, target_id):
        if not target_id in self.infohash_peers:
            self.infohash_peers[target_id] = {}

        peer_ip_port_strs = response['r']['values']

        #print "Got a list of %d peers" % len(peer_ip_port_strs)

        new_peers = []
        for ip_port_str in peer_ip_port_strs:
            ip_bytes, port = unpack("!4sH",  ip_port_str)
            ip_str = '.'.join(map(str, map(ord, ip_bytes)))

            new_peers.append((ip_str, port))

        for p in new_peers:
            self.infohash_peers[target_id][p] = time.time()

        #logging.info("<%s----%s---%s PONG\n" % (self.id.encode('hex'), id(self.get_peers_callbacks), id(self.get_peers_callbacks)))
        if target_id in self.get_peers_callbacks:
            for callback in self.get_peers_callbacks[target_id]:
                callback(target_id, new_peers, self.infohash_peers[target_id])
        #print "Peer list for hash:%s is:\n%s" % (target_id, str(self.infohash_peers[target_id]))

        #~~~~~~~~~~~~~~~~ MESSAGE: announce_peer
        #announce_peers Query = {"t":"aa", "y":"q", "q":"announce_peer", "a": {"id":"abcdefghij0123456789", "info_hash":"mnopqrstuvwxyz123456", "port": 6881, "token": "aoeusnth"}}
        #Response = {"t":"aa", "y":"r", "r": {"id":"mnopqrstuvwxyz123456"}}
    def announce_peer(self, info_hash, ip_port, response):
        """
        发送announce_peer请求
        """
        token = response['r']['token']
        #logging.info("ANNOUNCE_PEER ----%s--->\n" % str(ip_port))
        t_id = self.get_trasaction_id()
        announce_peer_msg = {"t": t_id, "y": "q", "q": "announce_peer", "a": {"id": self.id, "info_hash": info_hash, "token": token, "port": self.port}}
        self.sock.sendto(bencode(announce_peer_msg), ip_port)
        self.queries[t_id] = DHTQuery(t_id=announce_peer_msg['t'], a_info_hash=announce_peer_msg['a']['info_hash'], q=announce_peer_msg['q'])

    def got_announce_peer_query(self, query, source_ip_port):
        """
        别人发来的请求
        """
        # announce_peers Query = {"t":"aa", "y":"q", "q":"announce_peer", "a": {"id":"abcdefghij0123456789", "info_hash":"mnopqrstuvwxyz123456", "port": 6881, "token": "aoeusnth"}}
        info_hash = query['a']['info_hash']
        port = int(query['a']['port'])
        # 添加infohash到数据库###
        logging.info('announceinfo_hash: %s' % info_hash.encode("hex"))

        # logging.info("<%s----%s---%s PONG\n" % (self.id.encode('hex'), id(self.get_peers_callbacks), id(self.get_peers_callbacks)))
        self.infohash_peers[info_hash] = {}
        self.infohash_peers[info_hash][(source_ip_port[0], port)] = time.time()

        insert_into_database(info_hash, URL)

    def got_announce_peer_response(self, response, source_ip_port):
        """
        发送announce_peer后 返回的消息处理
        """
        transaction_id = response["t"]
        info_hash = self.get_original_info_hash_from_response(response)

        self.queries[transaction_id].free()
        del self.queries[transaction_id]
        logging.info("<return----%s--- ANNOUNCE_PEER \n" % info_hash.encode("hex"))

    def handle_response(self, response, source_ip_port):
        """
        处理返回的数据包，通过判断q来分配到不同的处理方法
        """
        t_id = response["t"]

        try:
            original_query = self.queries[t_id].q
            # logging.info("t_id:%s" % repr(unpack("H", t_id)))
        except KeyError:
            # import pdb; pdb.set_trace()
            # logging.info("I dont have a transaction ID that matches this response---%s" % unpack('H', t_id))
            # logging.info("I dont have a transaction ID!  %s,%s" % (repr(source_ip_port), response))
            # logging.info("t_id:%s" % repr(unpack("H", t_id)))
            # logging.info("t_id find error")
            # logging.info("Response for announce_response has not in querries\n %s" % str(source_ip_port))
            return

        if original_query == "ping":
            self.got_ping_response(response, source_ip_port)
        elif original_query == "find_node":
            # print "response:", original_query['q']
            self.got_find_node_response(response, source_ip_port)
        elif original_query == "get_peers":
            # print "response:", original_query['q']
            self.got_get_peers_response(response, source_ip_port)
        elif original_query == "announce_peer":
            print "response:", original_query
            self.got_announce_peer_response(response, source_ip_port)
        else:
            logging.info("q is not match")

    def handle_query(self, query, source_ip_port):
        """
        别人请求的数据包，通过q分配不同的处理方法
        """
        if query["q"] == "ping":
            self.got_ping_query(query, source_ip_port)
        elif query["q"] == "find_node":
            self.got_find_node_query(query, source_ip_port)
        elif query["q"] == "get_peers":
            # print self.id.encode("hex"), "other request:", query['q']
            self.got_get_peers_query(query, source_ip_port)
        elif query["q"] == "announce_peer":
            # print self.id.encode("hex"), "other request:", query['q']
            self.got_announce_peer_query(query, source_ip_port)

    def mem_test(self):
        """
        """
        from meliae import scanner
        scanner.dump_all_objects('/tmp/dump%s.txt' % time.time())

    def handle_input(self, fd, events):
        """
        统一收到的包
        """
        # import pdb; pdb.set_trace()
        # print "."
        (data, source_ip_port) = self.sock.recvfrom(4096)
        try:
            bdict = bdecode(data)
        except:
            return

        if 'y' in bdict:
            # Got a response from some previous query
            if bdict["y"] == "r":
                self.handle_response(bdict, source_ip_port)

            # Got a query for something
            if bdict["y"] == "q":
                self.handle_query(bdict, source_ip_port)

    # def check_queries(self, t_id):
    #    if t_id in self.queries:
    #        v = self.queries[t_id]
    #        if (time.time() > v.time_sent + DHT.TRANSACTION_ID_TIMEOUT):
    #            self.queries[t_id].free()
    #            del self.queries[t_id]
    def check_queries(self):
        """
        #对self.queries里面的超时检测，占用内存很大,30秒检查一遍
        """
        for k in self.queries:
            if time.time() > self.queries[k].time_sent + DHT.TRANSACTION_ID_TIMEOUT:
                self.queries[k].free()
                del self.queries[k]
                #print "*" * 20, "del queries"
        self.io_loop.add_timeout(time.time() + DHT.TRANSACTION_ID_TIMEOUT, self.check_queries)

    # XXX: This could block on sock.sendto, maybe do non blocking
    def bootstrap(self):
        self.io_loop.add_timeout(time.time() + DHT.PONG_TIMEOUT, self.bootstrap_by_finding_myself)

        # 对self.queries里面的超时检测，占用内存很大,30秒检查一遍
        # self.io_loop.add_timeout(time.time() + DHT.TRANSACTION_ID_TIMEOUT, self.check_queries)

        # self.io_loop.add_timeout(time.time() + 5, self.get_peers_test)
        # self.io_loop.add_timeout(time.time() + 5, partial(self.find_node, "\x00" * 20))

        self.insert_dump_peers()
        for ip_port in self.ip_ports:
            # print(ip_port)
            self.ping(ip_port)

    def insert_dump_peers(self):
        """
        导入dump的peers
        """
        path = "/tmp/peers.txt"
        if os.path.isfile(path):
            f = open(path)
            l = ast.literal_eval(f.read())
            for i in l:
                info_hash = i.decode('hex')
                self.infohash_peers[info_hash] = {}
                for p in l[i]:
                    self.infohash_peers[info_hash][p] = time.time()

    def start(self):
        self.io_loop.start()
