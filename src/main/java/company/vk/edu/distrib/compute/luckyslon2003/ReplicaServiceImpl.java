package company.vk.edu.distrib.compute.luckyslon2003;

import com.google.protobuf.ByteString;
import company.vk.edu.distrib.compute.luckyslon2003.proto.DeleteRequest;
import company.vk.edu.distrib.compute.luckyslon2003.proto.GetRequest;
import company.vk.edu.distrib.compute.luckyslon2003.proto.GetResponse;
import company.vk.edu.distrib.compute.luckyslon2003.proto.ReplicaServiceGrpc;
import company.vk.edu.distrib.compute.luckyslon2003.proto.UpsertRequest;
import company.vk.edu.distrib.compute.luckyslon2003.proto.WriteResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;

// gRPC-сервер ноды-реплики. Если нода "выключена" (имитация недоступности),
// любой вызов завершается статусом UNAVAILABLE -- так координатор отличает упавшую
// реплику от ответившей.
final class ReplicaServiceImpl extends ReplicaServiceGrpc.ReplicaServiceImplBase {

    private final ReplicaStorage storage;
    private final AtomicBoolean enabled;

    ReplicaServiceImpl(ReplicaStorage storage, AtomicBoolean enabled) {
        super();
        this.storage = storage;
        this.enabled = enabled;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> observer) {
        if (rejectIfDisabled(observer)) {
            return;
        }
        VersionedValue stored = storage.read(request.getId());
        GetResponse.Builder response = GetResponse.newBuilder();
        if (stored != null) {
            response.setPresent(true)
                    .setVersion(stored.version())
                    .setTombstone(stored.tombstone());
            if (!stored.tombstone()) {
                response.setValue(ByteString.copyFrom(stored.value()));
            }
        }
        observer.onNext(response.build());
        observer.onCompleted();
    }

    @Override
    public void upsert(UpsertRequest request, StreamObserver<WriteResponse> observer) {
        if (rejectIfDisabled(observer)) {
            return;
        }
        storage.upsert(request.getId(), request.getVersion(), request.getValue().toByteArray());
        observer.onNext(WriteResponse.getDefaultInstance());
        observer.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<WriteResponse> observer) {
        if (rejectIfDisabled(observer)) {
            return;
        }
        storage.delete(request.getId(), request.getVersion());
        observer.onNext(WriteResponse.getDefaultInstance());
        observer.onCompleted();
    }

    private boolean rejectIfDisabled(StreamObserver<?> observer) {
        if (enabled.get()) {
            return false;
        }
        observer.onError(Status.UNAVAILABLE.withDescription("replica disabled").asRuntimeException());
        return true;
    }
}
