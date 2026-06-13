package company.vk.edu.distrib.compute.luckyslon2003;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

// Координирует набор нод-реплик через внутренний gRPC-транспорт.
// Каждый ключ хранится на всех репликах; чтение/запись подтверждается кворумом ack.
final class ReplicaCoordinator {

    private static final String HOST = "localhost";

    private final Logger log = LoggerFactory.getLogger(ReplicaCoordinator.class);
    private final int replicas;
    private final List<ReplicaNode> nodes = new ArrayList<>();
    private final List<ReplicaProxy> proxies = new ArrayList<>();
    private final AtomicLong versions = new AtomicLong();

    ReplicaCoordinator(int replicas) {
        this.replicas = replicas;
        for (int i = 0; i < replicas; i++) {
            nodes.add(new ReplicaNode());
        }
    }

    void start() throws IOException {
        for (ReplicaNode node : nodes) {
            node.start();
            proxies.add(new ReplicaProxy(HOST, node.grpcPort()));
        }
    }

    void stop() {
        for (ReplicaProxy proxy : proxies) {
            proxy.close();
        }
        for (ReplicaNode node : nodes) {
            node.stop();
        }
    }

    int numberOfReplicas() {
        return replicas;
    }

    void setEnabled(int nodeId, boolean value) {
        nodes.get(nodeId).setEnabled(value);
    }

    long storedKeys(int nodeId) {
        return nodes.get(nodeId).storedKeys();
    }

    long reads(int nodeId) {
        return nodes.get(nodeId).reads();
    }

    long writes(int nodeId) {
        return nodes.get(nodeId).writes();
    }

    ReadOutcome read(String id, int ack) {
        List<ReplicaProxy.ReadResult> responses = new ArrayList<>();
        for (ReplicaProxy proxy : proxies) {
            try {
                responses.add(proxy.get(id));
            } catch (ReplicaUnavailableException e) {
                log.debug("Replica unavailable on read id={}", id, e);
            }
        }
        if (responses.size() < ack) {
            return ReadOutcome.unavailable();
        }
        ReplicaProxy.ReadResult latest = null;
        for (ReplicaProxy.ReadResult result : responses) {
            if (result.present() && (latest == null || result.version() > latest.version())) {
                latest = result;
            }
        }
        if (latest == null || latest.tombstone()) {
            return ReadOutcome.absent();
        }
        return ReadOutcome.found(latest.value());
    }

    boolean write(String id, byte[] value, int ack) {
        long version = versions.incrementAndGet();
        return replicate(proxy -> proxy.upsert(id, version, value)) >= ack;
    }

    boolean remove(String id, int ack) {
        long version = versions.incrementAndGet();
        return replicate(proxy -> proxy.delete(id, version)) >= ack;
    }

    private int replicate(Consumer<ReplicaProxy> operation) {
        int confirmed = 0;
        for (ReplicaProxy proxy : proxies) {
            try {
                operation.accept(proxy);
                confirmed++;
            } catch (ReplicaUnavailableException e) {
                log.debug("Replica unavailable on write", e);
            }
        }
        return confirmed;
    }

    static final class ReadOutcome {

        private static final ReadOutcome UNAVAILABLE = new ReadOutcome(false, false, new byte[0]);
        private static final ReadOutcome ABSENT = new ReadOutcome(true, false, new byte[0]);

        private final boolean quorum;
        private final boolean found;
        private final byte[] value;

        private ReadOutcome(boolean quorum, boolean found, byte[] value) {
            this.quorum = quorum;
            this.found = found;
            this.value = value.clone();
        }

        static ReadOutcome unavailable() {
            return UNAVAILABLE;
        }

        static ReadOutcome absent() {
            return ABSENT;
        }

        static ReadOutcome found(byte[] value) {
            return new ReadOutcome(true, true, value);
        }

        boolean quorum() {
            return quorum;
        }

        boolean found() {
            return found;
        }

        byte[] value() {
            return value.clone();
        }
    }
}
