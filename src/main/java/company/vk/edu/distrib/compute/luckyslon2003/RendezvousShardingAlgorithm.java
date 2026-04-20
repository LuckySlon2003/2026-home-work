package company.vk.edu.distrib.compute.luckyslon2003;

import java.util.List;

public class RendezvousShardingAlgorithm implements ShardingAlgorithm {
    private final List<String> endpoints;

    public RendezvousShardingAlgorithm(List<String> endpoints) {
        this.endpoints = List.copyOf(endpoints);
    }

    @Override
    public String primaryOwner(String key) {
        String bestEndpoint = null;
        long bestScore = Long.MIN_VALUE;
        for (String endpoint : endpoints) {
            long score = HashUtils.hash64(key + '\n' + endpoint);
            if (bestEndpoint == null || score > bestScore) {
                bestEndpoint = endpoint;
                bestScore = score;
            }
        }
        return bestEndpoint;
    }

    @Override
    public String name() {
        return "rendezvous";
    }
}
