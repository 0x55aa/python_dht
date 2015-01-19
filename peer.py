# coding: utf-8
import time
import logging
from functools import partial


PONG_TIMEOUT = 60*15


#Returns a iterator that will iterate bit by bit over a string!
def string_bit_iterator(str_to_iterate):
    bitmask = 128  # 1 << 7 or '0b10000000'
    cur_char_index = 0

    while cur_char_index < len(str_to_iterate):
        if bitmask & ord(str_to_iterate[cur_char_index]):
            yield 1
        else:
            yield 0

        bitmask = bitmask >> 1
        if bitmask == 0:
            bitmask = 128   # 1 << 7 or '0b10000000'
            cur_char_index = cur_char_index + 1


class DHTBucket(object):
    """
    桶
    """
    #__slots__ =
    #['key', 'value', 'left', 'right', 'last_time_validity_checked']
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.left = None
        self.right = None
        self.last_time_validity_checked = 0  # time.time()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        ret_str = ""
        if self.value:
            ret_str += "V(%s)  " % self.value
        #if self.left:
        ret_str += "L(%s)  " % self.left
        #if self.right:
        ret_str += "R(%s)" % self.right

        return ret_str

    def __getitem__(self, key):
        """
        x.__getitem__(key) <==> x[key], where key is 0 (left) or 1 (right)
        """
        return self.left if key == 0 else self.right

    def __setitem__(self, key, value):
        """
        x.__setitem__(key, value) <==>
        x[key]=value, where key is 0 (left) or 1 (right)
        """
        if key == 0:
            self.left = value
        else:
            self.right = value

    def remove_by_attribute(self, attr, value):
        for n in self.value:
            if getattr(n, attr, '') == value:
                self.value.remove(n)
                break

    def is_full(self):
        return len(self.value) >= DHTTree.MAX_LIST_LENGTH

    def free(self):
        self.left = None
        self.right = None
        self.value = None
        self.key = None


class DHTPeer(object):
    """
    节点保存在这里
    id ip port
    """
    def __init__(self, id, ip_port):
        self.id = id
        self.ip_port = ip_port

    def __hash__(self):
        return self.id

    def __cmp__(self, other):
        return str.__cmp__(self.id, other.id)

    def __eq__(self, other):
        return str.__eq__(self.id, other.id)

    def __iter__(self):
        return string_bit_iterator(self.id)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "ID:%s IP:%s PORT:%s" % \
            (self.id.encode("hex"), self.ip_port[0], self.ip_port[1])


class DHTTree(object):
    """
    保存所有节点
    """
    MAX_LIST_LENGTH = 8

    def __init__(self):
        self._root = DHTBucket(None, None)
        self._add_branches_to_node(self._root)
        self.bitmask = 1 << (20 * 8 - 1)
        # self.lenght = 0

    def _add_branches_to_node(self, node):
        node.left = DHTBucket(0, [])
        node.right = DHTBucket(1, [])

    #Response for find_node, iterate down the tree
    #as far as possible to get a bucket to retunr
    #Takes a string of bytes (represnting a DHT Node ID)
    #Or a DHTPeer because it implements __iter__
    def get_target_bucket(self, target):
        if isinstance(target, basestring):
            key = string_bit_iterator(target)
        elif isinstance(target, DHTPeer):
            key = target
        else:
            logging.error("Target must be either a string or DHTPeer type")
            return -1

        #This should iterate down the same side of a tree as the key provided.
        #It should then return up the list of nodes it finds. It then adds
        #nodes from the opposing branch from the bottom up until the list is
        #DHTTree.MAX_LIST_LENGTH long
        def search_tree(key, cur_node):
            try:
                b = key.next()
            except Exception, e:
                logging.error("Fell off the bottom of the DHT routing tree.")
                raise e

            next_node = cur_node[b]

            if not next_node:
                return cur_node.value
            else:
                that_side = search_tree(key, next_node)
                if that_side and len(that_side) < DHTTree.MAX_LIST_LENGTH:
                    # XXX: Could make thius bigger as an optimazaiotn
                    other_side_of_tree = cur_node[b ^ 1]
                    return that_side + search_tree(key, other_side_of_tree)
                else:
                    return that_side

        return search_tree(key, self._root)

    #Set a timeout that this bucket was last checked
    #If the time has passed then check the buket again
    #When checking the bucket ping all nodes and remember transaction IDS
    #A few seconds later go and make sure all those pongs came back via the
    #transaction IDS
    def find_non_responsive_node(self, cur_node, new_node, dht):
        if time.time() < cur_node.last_time_validity_checked + PONG_TIMEOUT:
            # I updated this bucket a few seconds ago.Dont bother those peers again
            return
        cur_node.last_time_validity_checked = time.time()

        def check_transactions(ping_transactions, new_node, cur_node):
            for count, transaction_id in enumerate(ping_transactions):
                if transaction_id in dht.queries:
                    node_to_remove_ip_port = dht.queries[transaction_id].ip_port
                    # print "Removing %s" % len(cur_node.value)
                    cur_node.remove_by_attribute("ip_port", node_to_remove_ip_port)
                    # print "Removing %s" % len(cur_node.value)
                if not cur_node.is_full():
                    cur_node.value.append(new_node)

        ping_transactions = []
        for n in cur_node.value:
            ping_transactions.append(dht.ping(n.ip_port))

        dht.io_loop.add_timeout(time.time() + PONG_TIMEOUT, partial(check_transactions, ping_transactions, new_node, cur_node))

    def insert(self, dht_node, dht):
        """
        dht为dht对象，主要是用来查找非活跃节点的时候用到ioloop和ping操作
         没判断重复
        """
        other_itr = dht_node.__iter__()
        my_iter = string_bit_iterator(dht.id)

        cur_node = self._root
        same_branch = True

        while cur_node is not None:
            other_next_bit = other_itr.next()

            if same_branch:
                same_branch = not(my_iter.next() ^ other_next_bit)

            cur_node = cur_node[other_next_bit]

            if cur_node and (cur_node.value is not None):
                # self.lenght += 1
                if cur_node.is_full():
                    # import pdb; pdb.set_trace()
                    if same_branch:
                        nodes_to_re_add = cur_node.value
                        cur_node.value = None
                        self._add_branches_to_node(cur_node)
                        for n in nodes_to_re_add:
                            # print "Readding %s" % n
                            self.insert(n, dht)
                        self.insert(dht_node, dht)
                        # print "Done"
                        break
                    else:
                        # print "Before:%s" % str(cur_node.value)
                        self.find_non_responsive_node(cur_node, dht_node, dht)
                        # print "After:%s" % str(cur_node.value)
                        break
                else:
                    if dht_node not in cur_node.value:
                        cur_node.value.append(dht_node)
                        # logging.info(str(cur_node.value))
                        break

            # print("loop")


dht_tree = DHTTree()
get_peers_callbacks = {}
infohash_peers = {}
#queries = {}
