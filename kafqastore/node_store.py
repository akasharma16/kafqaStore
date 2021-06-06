class KafqaStoreNode:
    """
        Implements a distributed key-value store's node.
    """

    def __init__(self, name, host, port='80'):
        self.name = name
        self.host = host
        self.port = port

        self.hashtable = dict()
        self.reverse_lookup_hashtables = dict()
        return

    def set(self, key: str, json_value: dict):
        # #### data type checking and populate secondary index
        for attribute, value in json_value.items():
            # #### check if hashtable for this attribute already exists
            if self.reverse_lookup_hashtables.get(attribute):
                # Using this attribute specific dictionary to store attribute datatype too
                datatype = self.reverse_lookup_hashtables[attribute].get('datatype')

                # validate datatype
                if not type(value) == datatype:
                    raise Exception("Attribute datatype mismatch")

                # #### check if values for the same key already exist
                l = self.reverse_lookup_hashtables[attribute].get(value)
                if l:
                    self.reverse_lookup_hashtables[attribute][value] = l.append(key)
                else:
                    # adding key for the first time
                    self.reverse_lookup_hashtables[attribute].update({value: [key]})
            else:
                self.reverse_lookup_hashtables[attribute] = {value: [key]}  # first entry
                # #### setting datatype for the first insert
                self.reverse_lookup_hashtables[attribute]['datatype'] = type(value)

        self.hashtable[key] = json_value

    def get(self, key: str) -> dict:
        return self.hashtable.get(key, None)

    def delete(self, key: str):
        self.hashtable.pop(key)
        # #### TODO: delete keys from secondary index too

    def reverse_lookup_shard(self, key: str, attribute: str) -> list:
        keys = self.reverse_lookup_hashtables.get(attribute, {}).get(key, [])
        # verify keys still exist
        valid_keys = []
        for k in keys:
            if self.hashtable.get(k):
                valid_keys.append(k)
        return valid_keys
