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

    def test_get_and_set(self):
        key = 'test1'
        value = {
            'prop1': 'yoyo',
            'prop2': 'singh',
        }
        self.hr.set_key(key, value)
        assert self.hr.get_key(key) is value

    def test_delete(self):
        key = 'test2'
        value = {
            'prop1': 'steve',
            'prop2': 'jobs',
        }
        self.hr.set_key(key, value)
        assert self.hr.get_key(key) is value
        self.hr.del_key(key)
        assert self.hr.get_key(key) is None

    def test_reverse(self):
        key = 'test3'
        value = {
            'reverse': 'lookup',
            'prop2': 'jobs',
        }
        self.hr.set_key(key, value)
        assert self.hr.reverse_lookup(value='lookup', attribute='reverse') == ['test3']


t = KafqaStoreTests()
t.test_get_and_set()
t.test_delete()
t.test_reverse()
