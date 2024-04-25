# Consistent Hashing Implementation

Consistent Hashing is used by Cassandra to map the partition keys to nodes. 

One advantage of consistent hashing is not all keys need to be remapped to node upon resizing the cluster. If n is number of nodes in cluster, only 1/n keys need to remapped. 






## Installation

The implementation class is Murmur3ConsistentHasher. Junits are added with intent to test the Murmur3ConsistentHasher functionalities.