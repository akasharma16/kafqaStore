import hashlib
from bisect import bisect_right, bisect, bisect_left

from node_store import KafqaStoreNode


class HashRing:
    """
        Implements a consistent hashing scheme for sharding.
    """

    def __init__(self):
        self.node_ring_indices = []           # sorted array of indices taken up in the ring
        self.nodes = []           # nodes present in the ring. nodes[i] is present at keys[i] on the ring
        self.total_ring_indices = pow(2, 16)     # total slots in the ring

    def get_ring_index_for_key(self, key: str) -> int:
        """
            Creates an integer equivalent of a SHA256 hash of the given key.
            Then takes a modulo with the total number of slots in hash space.
        """
        # #### converting key into bytes and passing it to hash function
        hsh = hashlib.sha256()
        hsh.update(bytes(key.encode('utf-8')))

        # converting the base 16 digest of bytes into equivalent integer value and returning modulo
        return int(hsh.hexdigest(), 16) % self.total_ring_indices

    def get_node_for_a_key(self, key: str) -> KafqaStoreNode:
        """
            Given key, the function returns the node it should be associated with using binary search.
        """
        ring_index = self.get_ring_index_for_key(key)

        # we find the first node to the right of this key
        index = bisect_right(self.node_ring_indices, ring_index) % len(self.node_ring_indices)

        # return the node present at the index
        return self.nodes[index]

    def set_key(self, key, value):
        node = self.get_node_for_a_key(key)
        node.set(key=key, json_value=value)  # this will be an RPC once distributed

    def get_key(self, key):
        node = self.get_node_for_a_key(key)
        return node.get(key=key)

    def del_key(self, key):
        node = self.get_node_for_a_key(key)
        node.delete(key=key)

    def reverse_lookup(self, value: str, attribute: str) -> list:
        # #### instead of routing to a shard, we need to accumulate results
        keys = []
        for n in self.nodes:
            keys = keys + n.reverse_lookup_shard(value, attribute)
        return keys

    def add_node(self, node: KafqaStoreNode) -> int:
        """
            Add a node to the ring and migrate relevant keys to the new node.
        """
        # #### checking if hash space is full
        if len(self.node_ring_indices) == self.total_ring_indices:
            raise Exception('Hash space is full!')

        new_node_ring_index = self.get_ring_index_for_key(key=node.host)  # to keep node unique on the ring

        # #### insert new node's new_node_ring_index value in the filled_ring_indices array
        new_node_array_index = bisect(self.node_ring_indices, new_node_ring_index)

        # #### check collision case
        if new_node_array_index > 0 and \
                self.node_ring_indices[new_node_array_index - 1] == new_node_ring_index:
            raise Exception('Key Collision!')

        # migrate_date()  # TODO

        # #### Update ring
        self.nodes.insert(new_node_array_index, node)
        self.node_ring_indices.insert(new_node_array_index, new_node_ring_index)

        return new_node_ring_index

    def rm_node(self, node: KafqaStoreNode) -> int:
        """
            Remove a node from the ring and migrate relevant keys to another node.
        """
        # #### checking if hash space has any node to remove
        if len(self.node_ring_indices) == 0:
            raise Exception('Hash space is empty!')

        ring_node_index = self.get_ring_index_for_key(node.host)
        current_array_index_of_node = bisect_left(self.node_ring_indices, ring_node_index)

        # #### check node exists
        if current_array_index_of_node >= len(self.node_ring_indices) or \
                self.node_ring_indices[current_array_index_of_node] is not ring_node_index:
            raise Exception('Node does not exist!')

        # migrate_date()  # TODO

        # #### Update ring
        self.nodes.pop(current_array_index_of_node)
        self.node_ring_indices.pop(current_array_index_of_node)

        return ring_node_index


# #### TODO: Node heartbeat
