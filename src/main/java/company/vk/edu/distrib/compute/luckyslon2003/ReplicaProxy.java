package company.vk.edu.distrib.compute.luckyslon2003;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.luckyslon2003.proto.DeleteRequest;
import company.vk.edu.distrib.compute.luckyslon2003.proto.GetRequest;
import company.vk.edu.distrib.compute.luckyslon2003.proto.GetResponse;
import company.vk.edu.distrib.compute.luckyslon2003.proto.ReplicaServiceGrpc;
import company.vk.edu.distrib.compute.luckyslon2003.proto.UpsertRequest;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;

// gRPC-клиент координатора к одной ноде-реплике. Прячет всю работу с gRPC/protobuf,
// наружу отдаёт простые доменные операции get/upsert/delete.
final class ReplicaProxy {

    private final ManagedChannel channel;
    private final ReplicaServiceGrpc.ReplicaServiceBlockingStub stub;

    ReplicaProxy(String host, int grpcPort) {
        this.channel = Grpc.newChannelBuilderForAddress(host, grpcPort, InsecureChannelCredentials.create())
                .build();
        this.stub = ReplicaServiceGrpc.newBlockingStub(channel);
    }

    ReadResult get(String id) {
        try {
            GetResponse response = stub.get(GetRequest.newBuilder().setId(id).build());
            return new ReadResult(
                    response.getPresent(),
                    response.getVersion(),
                    response.getTombstone(),
                    response.getValue().toByteArray());
        } catch (StatusRuntimeException e) {
            throw new ReplicaUnavailableException(e);
        }
    }

    void upsert(String id, long version, byte[] value) {
        try {
            stub.upsert(UpsertRequest.newBuilder()
                    .setId(id)
                    .setVersion(version)
                    .setValue(ByteString.copyFrom(value))
                    .build());
        } catch (StatusRuntimeException e) {
            throw new ReplicaUnavailableException(e);
        }
    }

    void delete(String id, long version) {
        try {
            stub.delete(DeleteRequest.newBuilder().setId(id).setVersion(version).build());
        } catch (StatusRuntimeException e) {
            throw new ReplicaUnavailableException(e);
        }
    }

    void close() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static final class ReadResult {

        private final boolean keyPresent;
        private final long revision;
        private final boolean deleted;
        private final byte[] payload;

        ReadResult(boolean present, long version, boolean tombstone, byte[] value) {
            this.keyPresent = present;
            this.revision = version;
            this.deleted = tombstone;
            this.payload = value.clone();
        }

        boolean present() {
            return keyPresent;
        }

        long version() {
            return revision;
        }

        boolean tombstone() {
            return deleted;
        }

        byte[] value() {
            return payload.clone();
        }
    }
}
