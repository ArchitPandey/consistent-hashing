package org.example.consistenthash;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

@Slf4j
public class Murmur3ConsistentHasherTest {

    @BeforeEach
    public void setup() {

    }

    @Test
    public void tokenRangesValidation() {
        IConsistentHasher consistentHasher = new Murmur3ConsistentHasher(3, Arrays.asList("10.0.0.0", "10.0.0.1", "10.0.0.2"));

        Map<String, List<TokenRange>> nodeTokenRangesMap = consistentHasher.getTokenRangeForAllNodes();

        for(Map.Entry<String, List<TokenRange>> entry: nodeTokenRangesMap.entrySet()) {
            log.info("Token ranges for {} ::", entry.getKey());
            for(TokenRange tokenRange: entry.getValue()) {
                log.info(tokenRange.toString());
            }
        }

        Assertions.assertNotNull(nodeTokenRangesMap);

    }

    @Test
    public void dataDistributionTest() {
        String node1 = "10.0.0.0";
        String node2 = "10.0.0.1";
        String node3 = "10.0.0.2";

        IConsistentHasher consistentHasher = new Murmur3ConsistentHasher(
                4,
                Arrays.asList(node1, node2, node3)
        );

        Map<String, Integer> datastore = new HashMap<>();
        datastore.put(node1, 0);
        datastore.put(node2, 0);
        datastore.put(node3, 0);

        //generate 50k random keys and add them to
        //datastore using murmur3ConsistentHasher
        int totalKeys = 50000;
        for (int i=0; i<totalKeys; i++) {
            String randomKey = RandomStringUtils.randomAlphabetic(i%50);
            String nodeForRandomKey = consistentHasher.getNodeForKey(randomKey);
            Integer keyCountInNode = datastore.get(nodeForRandomKey);
            datastore.put(nodeForRandomKey, keyCountInNode+1);
        }

        float dataPercentInNode1 = dataPercentForNode(datastore.get(node1), totalKeys);
        float dataPercentInNode2 = dataPercentForNode(datastore.get(node2), totalKeys);
        float dataPercentInNode3 = dataPercentForNode(datastore.get(node3), totalKeys);

        log.info("total number of keys: {}", totalKeys);
        log.info("data percent node 1: {}", dataPercentInNode1);
        log.info("data percent node 2: {}", dataPercentInNode2);
        log.info("data percent node 3: {}", dataPercentInNode3);

        Assertions.assertTrue( dataPercentInNode1 > 0.0);
        Assertions.assertTrue( dataPercentInNode2 > 0.0);
        Assertions.assertTrue( dataPercentInNode3 > 0.0);
    }

    @Test
    public void keyGetSetTest() {
        IConsistentHasher consistentHasher = new Murmur3ConsistentHasher(
                4,
                Arrays.asList("10.0.0.0", "10.0.0.1", "10.0.0.2")
        );

        Map<String, Set<String>> datastore = new HashMap<>();
        datastore.put("10.0.0.0", new HashSet<>());
        datastore.put("10.0.0.1", new HashSet<>());
        datastore.put("10.0.0.2", new HashSet<>());

        //add keys to datastore
        String testKey1 = "TESTKEY1";
        String nodeAddress1 = consistentHasher.getNodeForKey(testKey1);
        log.info("node for key: {} is {}", testKey1, nodeAddress1);
        datastore.get(nodeAddress1).add(testKey1);

        String testKey2 = "TESTKEY2";
        String nodeAddress2 = consistentHasher.getNodeForKey(testKey2);
        log.info("node for key: {} is {}", testKey2, nodeAddress2);
        datastore.get(nodeAddress2).add(testKey2);

        String testKey3 = "TESTKEY3";
        String nodeAddress3 = consistentHasher.getNodeForKey(testKey3);
        log.info("node for key: {} is {}", testKey3, nodeAddress3);
        datastore.get(nodeAddress3).add(testKey3);


        //fetch keys back from datastore
        String nodeForTestKey1 = consistentHasher.getNodeForKey(testKey1);
        Assertions.assertTrue(datastore.get(nodeForTestKey1).contains(testKey1));

        String nodeForTestKey2 = consistentHasher.getNodeForKey(testKey2);
        Assertions.assertTrue(datastore.get(nodeForTestKey2).contains(testKey2));

        String nodeForTestKey3 = consistentHasher.getNodeForKey(testKey3);
        Assertions.assertTrue(datastore.get(nodeForTestKey3).contains(testKey3));
    }

    @Test
    public void addNodeTest() {
        String node1 = "10.0.0.0";
        String node2 = "10.0.0.1";
        String node3 = "10.0.0.2";

        IConsistentHasher consistentHasher = new Murmur3ConsistentHasher(
                4,
                Arrays.asList(node1, node2, node3)
        );

        Map<String, TreeMap<Integer, String>> datastore = new HashMap<>();
        datastore.put(node1, new TreeMap<Integer, String>());
        datastore.put(node2, new TreeMap<Integer, String>());
        datastore.put(node3, new TreeMap<Integer, String>());


        //generate 50k random keys and add them to
        //datastore using murmur3ConsistentHasher
        int totalKeys = 50000;
        Random randomLenGenerator = new Random();
        for (int i=0; i<totalKeys; i++) {
            int randomLen = randomLenGenerator.nextInt(10)+1;
            String randomKey = RandomStringUtils.randomAlphabetic(randomLen);
            int randomKeyHash = consistentHasher.getHashForKey(randomKey);
            String nodeForRandomKey = consistentHasher.getNodeForKey(randomKey);
            datastore.get(nodeForRandomKey).put(randomKeyHash, randomKey);
        }

        float dataPercentInNode1 = dataPercentForNode(datastore.get(node1).size(), totalKeys);
        float dataPercentInNode2 = dataPercentForNode(datastore.get(node2).size(), totalKeys);
        float dataPercentInNode3 = dataPercentForNode(datastore.get(node3).size(), totalKeys);

        //note that data percent maynot add to upto 100%
        //this is because the way we're storing data
        //multiple keys can have some hash, but we store only
        //one key per hash in datastore TreeMap<Integer, String>
        //structure
        log.info("total number of keys: {}", totalKeys);
        log.info("data percent node 1: {}", dataPercentInNode1);
        log.info("data percent node 2: {}", dataPercentInNode2);
        log.info("data percent node 3: {}", dataPercentInNode3);

        //adding a node to consistent hash ring
        String node4 = "10.0.0.3";
        consistentHasher.addNode(node4);
        datastore.put(node4, new TreeMap<>());

        List<TokenRange> tokenRangesNewNode = consistentHasher.getTokenRangeForNode(node4);

        for(TokenRange tokenRange: tokenRangesNewNode) {
            int start = tokenRange.getStartToken();
            int end = tokenRange.getEndToken();
            copyDataOverForTokenRanges(start, end, datastore.get(node1), datastore.get(node4));
            copyDataOverForTokenRanges(start, end, datastore.get(node2), datastore.get(node4));
            copyDataOverForTokenRanges(start, end, datastore.get(node3), datastore.get(node4));
        }

        dataPercentInNode1 = dataPercentForNode(datastore.get(node1).size(), totalKeys);
        dataPercentInNode2 = dataPercentForNode(datastore.get(node2).size(), totalKeys);
        dataPercentInNode3 = dataPercentForNode(datastore.get(node3).size(), totalKeys);
        float dataPercentInNode4 = dataPercentForNode(datastore.get(node4).size(), totalKeys);

        log.info("new data distribution:");
        log.info("data percent node 1: {}", dataPercentInNode1);
        log.info("data percent node 2: {}", dataPercentInNode2);
        log.info("data percent node 3: {}", dataPercentInNode3);
        log.info("data percent node 4: {}", dataPercentInNode4);

        Assertions.assertTrue( dataPercentInNode1 > 0.0);
        Assertions.assertTrue( dataPercentInNode2 > 0.0);
        Assertions.assertTrue( dataPercentInNode3 > 0.0);
        Assertions.assertTrue( dataPercentInNode4 > 0.0);
    }

    private void copyDataOverForTokenRanges(int start, int end, TreeMap<Integer, String> src, TreeMap<Integer, String> dest) {
        NavigableMap<Integer, String> subMap = src.subMap(start, true, end, true);
        Map<Integer, String> tmpMap = new HashMap<>();

        for(Map.Entry<Integer, String> subMapEntry: subMap.entrySet()) {
            tmpMap.put(subMapEntry.getKey(), subMapEntry.getValue());
        }

        //use a tmp map to avoid concurrent modification exception
        for(Map.Entry<Integer, String> tmpMapEntry: tmpMap.entrySet()) {
            dest.put(tmpMapEntry.getKey(), tmpMapEntry.getValue());
            src.remove(tmpMapEntry.getKey());
        }
    }

    private float dataPercentForNode(int dataSizeInNode, int totalData) {
        return ( ( (float) dataSizeInNode * 100) / (float) totalData );
    }

}
