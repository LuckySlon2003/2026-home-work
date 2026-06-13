package company.vk.edu.distrib.compute.luckyslon2003;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

// Нода-реплика: gRPC-сервер на собственном порту + локальное хранилище.
// Это "узел кластера" во внутреннем транспорте; координатор обращается к нему по gRPC.
final class ReplicaNode {

    private final ReplicaStorage storage = new ReplicaStorage();
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final Server server;

    ReplicaNode() {
        this.server = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                .addService(new ReplicaServiceImpl(storage, enabled))
                .build();
    }

    void start() throws IOException {
        server.start();
    }

    void stop() {
        server.shutdownNow();
        try {
            server.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    int grpcPort() {
        return server.getPort();
    }

    void setEnabled(boolean value) {
        enabled.set(value);
    }

    long storedKeys() {
        return storage.storedKeys();
    }

    long reads() {
        return storage.reads();
    }

    long writes() {
        return storage.writes();
    }
}
