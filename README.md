# kafqaStore
A distributed key-value store supporting attributes and secondary indexing.

## In progress:
- Further test coverage
- Data migration across nodes when scaling up/down
- Redundancy clusters for High Availability with heartbeats and circuit breakers
- RPC layer for distributed hardware production deployments

## Major Architecture/Design Aspects of the code in further detail:
- **Scaling**: The system is designed to be distributed with data shards on independent nodes. Most important aspect of any such distributed system is the hashing algorithm used to map requests to individual nodes. Naive hashing algos like node-count modulo have the risk of entire data migration during scaling events. So a more consistent algo independent of the node-count is essential. One such algo using a large hash-ring has been used with a large hash space to avoid collisions with the node keys on the ring. 

- **Shared-nothing**: The individual nodes follow the shared-nothing design pattern and can be run independently. Each has its own data storage space and usage interface.

- **Thread safety**: Any data related system should consider thread-safety especially the ones expecting high throughput. This implementation being in python, the atomic execution locks are not explicitly used and python's GIL takes care of the race conditions internally. 

- **Reverse lookups**: Apart from direct lookups on keys that store and retrieve data in json formats, this implementation also allows reverse lookups based on values of a particular attribute in the json. This is achieved using a secondary index for each attribute at each node. The result from each of the individual hash table lookups is combined to return a cumulative list of keys,each of which contains the relevant attribute with the value provided in the query. 

- **Interfaces**: This implementation has the following interfaces:
  - get: To retrieve json data associated with a given key
  - set: To store a new key-value pair into the store.
  - delete: To delete a key from the distributed store
  - reverse-lookup: To get a list of keys that contain a given attribute as part of their json value and attribute value is as provided in the query.
  - add-node: To scale-up the system by adding a new node. 
  - rm-node: To scale-down the system by removing a new node.

## Future scope:
- Monitoring agents per node that stream out log data
- Weighted distribution based on node metrics
- Bloom filters to optimize further secondary index lookups
- Persisting data of individual nodes on disk and the subsequent required serializations 

