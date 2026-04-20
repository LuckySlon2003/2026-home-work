package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.nio.file.Path;
import java.util.List;

public class LuckySlon2003KVClusterFactory extends KVClusterFactory {
    private static final String STRATEGY_PROPERTY = "luckyslon2003.sharding.algorithm";
    private static final String VIRTUAL_NODES_PROPERTY = "luckyslon2003.sharding.virtualNodes";
    private static final int DEFAULT_VIRTUAL_NODES = 128;

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        List<String> endpoints = ports.stream()
                .map(port -> "http://localhost:" + port)
                .toList();
        ShardingAlgorithm algorithm = createAlgorithm(endpoints);
        return new LuckySlon2003KVCluster(ports, dataDirectory(), algorithm);
    }

    protected Path dataDirectory() {
        return Path.of(System.getProperty("java.io.tmpdir"), "luckyslon2003-cluster-data");
    }

    private ShardingAlgorithm createAlgorithm(List<String> endpoints) {
        String strategy = System.getProperty(STRATEGY_PROPERTY, "rendezvous").trim().toLowerCase(java.util.Locale.ROOT);
        return switch (strategy) {
            case "consistent" -> new ConsistentHashingShardingAlgorithm(endpoints, virtualNodesPerEndpoint());
            case "rendezvous" -> new RendezvousShardingAlgorithm(endpoints);
            default -> throw new IllegalArgumentException("Unsupported sharding algorithm: " + strategy);
        };
    }

    private int virtualNodesPerEndpoint() {
        return Integer.getInteger(VIRTUAL_NODES_PROPERTY, DEFAULT_VIRTUAL_NODES);
    }
}
