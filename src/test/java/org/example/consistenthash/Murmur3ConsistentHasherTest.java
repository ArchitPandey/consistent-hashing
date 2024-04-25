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
        IConsistentHasher consistentHasher = new Murmur3ConsistentHasher(
                4,
                Arrays.asList("10.0.0.0", "10.0.0.1", "10.0.0.2")
        );

        Map<String, Integer> datastore = new HashMap<>();
        datastore.put("10.0.0.0", 0);
        datastore.put("10.0.0.1", 0);
        datastore.put("10.0.0.2", 0);

        //generate 50k random keys and add them to
        //datastore using murmur3ConsistentHasher
        int totalKeys = 50000;
        for (int i=0; i<totalKeys; i++) {
            String randomKey = RandomStringUtils.randomAlphabetic(i%50);
            String nodeForRandomKey = consistentHasher.getNodeForKey(randomKey);
            Integer keyCountInNode = datastore.get(nodeForRandomKey);
            datastore.put(nodeForRandomKey, keyCountInNode+1);
        }

        float dataPercentInNode1 = ( ( (float) datastore.get("10.0.0.0") *100 ) /(float) totalKeys );
        float dataPercentInNode2 = ( ( (float) datastore.get("10.0.0.1") *100 ) /(float) totalKeys );
        float dataPercentInNode3 = ( ( (float) datastore.get("10.0.0.2") *100 ) /(float) totalKeys );

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

}
