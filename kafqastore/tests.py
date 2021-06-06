from distribute import HashRing
from node_store import KafqaStoreNode


class KafqaStoreTests:

    def __init__(self):
        self.nodes = [
            KafqaStoreNode(name='first', host='host1'),
            KafqaStoreNode(name='second', host='host2'),
        ]
        self.hr = HashRing()
        for n in self.nodes:
            self.hr.add_node(n)
        print(self.hr.node_ring_indices)

    def test_get(self):
        key = 'test1'
        value = {
            'prop1': 'yoyo',
            'prop2': 'singh',
        }
        self.hr.set_key(key, value)
        print(self.hr.get_key(key))
        assert self.hr.get_key(key) is value


t = KafqaStoreTests()
t.test_get()
