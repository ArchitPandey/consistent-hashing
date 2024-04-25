package org.example.consistenthash;

import java.util.List;
import java.util.Map;

public interface IConsistentHasher {

    String getNodeForKey(String key);

    void addNode(String nodeAddress);

    void removeNode(String nodeAddress);

    List<TokenRange> getTokenRangeForNode(String nodeAddress);

    Map<String, List<TokenRange>> getTokenRangeForAllNodes();

}
