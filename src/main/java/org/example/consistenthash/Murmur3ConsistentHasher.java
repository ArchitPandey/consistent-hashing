package org.example.consistenthash;

import org.apache.commons.codec.digest.MurmurHash3;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class Murmur3ConsistentHasher implements IConsistentHasher {

    int numVNodesPerPhysicalNode;
    Set<String> nodeAddresses;
    TreeMap<Integer, String> tokenToNodeMap;

    public Murmur3ConsistentHasher(int numVNodesPerPhysicalNode,
                                   List<String> nodeAddresses) {
        this.numVNodesPerPhysicalNode = numVNodesPerPhysicalNode;
        this.nodeAddresses = new HashSet<>();
        this.tokenToNodeMap = new TreeMap<Integer, String>();
        init(nodeAddresses);
    }

    /**
     * @param key
     * @return nodeAddress
     * calculate the keyhash. traverse clockwise in the ring.
     * key will be mapped to the first vnode encountered when
     * traversing clockwise
     */
    @Override
    public String getNodeForKey(String key) {
        int keyHash = murmur3Hash(key);
        Map.Entry<Integer, String> nextVNodeEntryOnRing = this.tokenToNodeMap.ceilingEntry(keyHash);

        //keyHash lies somewhere towards the end of the ring and there
        //is no vnode after keyHash. In this case key would map to the
        //first vnode encountered when going clockwise from the start of
        //the ring
        if(Objects.isNull(nextVNodeEntryOnRing) ) {
            return this.tokenToNodeMap.firstEntry().getValue();
        }

        return nextVNodeEntryOnRing.getValue();
    }

    /**
     * @param nodeAddress
     * calculate the vnodes for node to be added. any key
     * that maps to these vnodes will be stored in node
     *
     * vnodes are calculated by appending nodeAddress with an index
     * and then calculating the hash.
     *
     * https://tom-e-white.com/2007/11/consistent-hashing.html
     * https://github.com/zeromicro/go-zero/blob/master/core/hash/consistenthash.go
     *
     * Another approach uses an array of hash functions and
     * calculate hash of node address using each hash function. the
     * hash function outputs are vnodes on the consistent hash ring
     *
     * https://web.archive.org/web/20210308102408/https://theory.stanford.edu/%7Etim/s16/l/l1.pdf
     */
    @Override
    public void addNode(String nodeAddress) {

        for(int i=1; i<=this.numVNodesPerPhysicalNode; i++) {
            int vNodePosition = murmur3Hash(vNodeString(nodeAddress, i));
            this.tokenToNodeMap.put(vNodePosition, nodeAddress);
        }

        this.nodeAddresses.add(nodeAddress);
    }

    /**
     * @param nodeAddress
     *
     * remove all the vnodes for this node from the ring
     *
     * if a vnode, vx-i, is removed from the ring. the data stored
     * by that particular vnode would now need to be stored by the
     * next vnode encountered when traversing clockwise from vx-i
     *
     * the mechanism of data transfer is left on the systems using
     * this consistent hasher
     */
    @Override
    public void removeNode(String nodeAddress) {
        for (int i=1; i<this.numVNodesPerPhysicalNode; i++) {
            int vNodePosition = murmur3Hash(vNodeString(nodeAddress, i));
            this.tokenToNodeMap.remove(vNodePosition);
        }

        this.nodeAddresses.remove(nodeAddress);
    }

    /**
     * @param nodeAddress
     * @return tokenRanges[]
     *
     * return all the token ranges that map to nodeAddress
     */
    @Override
    public List<TokenRange> getTokenRangeForNode(String nodeAddress) {
        List<TokenRange> tokenRanges = new ArrayList<>();

        for(int i=1; i<=this.numVNodesPerPhysicalNode; i++) {
            int vNodePosition = murmur3Hash(vNodeString(nodeAddress, i));
            Integer previousVNode = this.tokenToNodeMap.lowerKey(vNodePosition);

            //if we don't find a vNode before vNodePosition
            //this means vNodePosition is the first vNode in
            //the ring. all tokens that the greater than highest
            //vnode (last one on the ring) and less than vNodePosition
            //are mapped to vNodePosition
            if ( Objects.isNull(previousVNode) ) {
                Integer lastVNode = this.tokenToNodeMap.lastKey();
                tokenRanges.add(new TokenRange(lastVNode+1, Integer.MAX_VALUE));
                tokenRanges.add(new TokenRange(Integer.MIN_VALUE, vNodePosition));
            } else {
                tokenRanges.add(new TokenRange(previousVNode+1, vNodePosition));
            }
        }

        return tokenRanges;
    }

    @Override
    public Map<String, List<TokenRange>> getTokenRangeForAllNodes() {
        Map<String, List<TokenRange>> nodeToTokenRangesMap = new HashMap<>();

        for(String nodeAddress: this.nodeAddresses) {
            List<TokenRange> tokenRanges = this.getTokenRangeForNode(nodeAddress);
            nodeToTokenRangesMap.put(nodeAddress, tokenRanges);
        }

        return nodeToTokenRangesMap;
    }

    private String vNodeString(String nodeAddress, int suffix) {
        return nodeAddress.concat(Integer.toString(suffix));
    }

    private int murmur3Hash(String key) {
        int hash = MurmurHash3.hash32x86(key.getBytes(StandardCharsets.UTF_8));
        return hash;
    }

    private void init(List<String> nodeAddresses) {
        for(String nodeAddress: nodeAddresses) {
            addNode(nodeAddress);
        }
    }
}
