package company.vk.edu.distrib.compute.luckyslon2003;

public interface ShardingAlgorithm {
    String primaryOwner(String key);

    String name();
}
