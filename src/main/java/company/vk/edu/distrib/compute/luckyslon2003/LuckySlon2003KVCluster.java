package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.KVCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LuckySlon2003KVCluster implements KVCluster {
    private static final Logger log = LoggerFactory.getLogger(LuckySlon2003KVCluster.class);

    private final List<Integer> ports;
    private final List<String> endpoints;
    private final Path baseDirectory;
    private final ShardingAlgorithm shardingAlgorithm;
    private final Map<String, LuckySlon2003KVService> activeNodes = new ConcurrentHashMap<>();

    public LuckySlon2003KVCluster(
            List<Integer> ports,
            Path baseDirectory,
            ShardingAlgorithm shardingAlgorithm
    ) {
        this.ports = List.copyOf(ports);
        this.endpoints = this.ports.stream()
                .map(port -> "http://localhost:" + port)
                .toList();
        this.baseDirectory = baseDirectory;
        this.shardingAlgorithm = shardingAlgorithm;
    }

    @Override
    public synchronized void start() {
        for (String endpoint : endpoints) {
            start(endpoint);
        }
    }

    @Override
    public synchronized void start(String endpoint) {
        if (activeNodes.containsKey(endpoint)) {
            return;
        }

        int port = portOf(endpoint);
        try {
            FileDao dao = new FileDao(baseDirectory.resolve(Integer.toString(port)));
            LuckySlon2003KVService service = new LuckySlon2003KVService(port, dao, endpoint, shardingAlgorithm);
            service.start();
            activeNodes.put(endpoint, service);
            if (log.isInfoEnabled()) {
                log.info("Started cluster node {} using {} hashing", endpoint, shardingAlgorithm.name());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to start cluster node " + endpoint, e);
        }
    }

    @Override
    public synchronized void stop() {
        for (String endpoint : List.copyOf(activeNodes.keySet())) {
            stop(endpoint);
        }
    }

    @Override
    public synchronized void stop(String endpoint) {
        LuckySlon2003KVService service = activeNodes.remove(endpoint);
        if (service == null) {
            return;
        }
        service.stop();
        if (log.isInfoEnabled()) {
            log.info("Stopped cluster node {}", endpoint);
        }
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    private int portOf(String endpoint) {
        int separator = endpoint.lastIndexOf(':');
        if (separator < 0 || separator == endpoint.length() - 1) {
            throw new IllegalArgumentException("Unsupported endpoint: " + endpoint);
        }
        return Integer.parseInt(endpoint.substring(separator + 1));
    }
}
