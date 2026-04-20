package company.vk.edu.distrib.compute.luckyslon2003;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashingShardingAlgorithm implements ShardingAlgorithm {
    private final NavigableMap<Long, String> ring;

    public ConsistentHashingShardingAlgorithm(List<String> endpoints, int virtualNodesPerEndpoint) {
        this.ring = new TreeMap<>();
        for (String endpoint : endpoints) {
            for (int vnode = 0; vnode < virtualNodesPerEndpoint; vnode++) {
                ring.put(HashUtils.hash64(endpoint + '#' + vnode), endpoint);
            }
        }
    }

    @Override
    public String primaryOwner(String key) {
        long hash = HashUtils.hash64(key);
        var entry = ring.ceilingEntry(hash);
        if (entry != null) {
            return entry.getValue();
        }
        return ring.firstEntry().getValue();
    }

    @Override
    public String name() {
        return "consistent";
    }
}
