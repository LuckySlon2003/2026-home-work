package company.vk.edu.distrib.compute.luckyslon2003;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

// In-memory хранилище одной ноды-реплики плюс счётчики обращений для /stats.
// Запись с меньшей версией никогда не затирает запись с большей -- порядок применения
// операций между репликами может различаться, но результат остаётся согласованным.
final class ReplicaStorage {

    private static final byte[] EMPTY = new byte[0];

    private final Map<String, VersionedValue> data = new ConcurrentHashMap<>();
    private final AtomicLong readCount = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();

    VersionedValue read(String id) {
        readCount.incrementAndGet();
        return data.get(id);
    }

    void upsert(String id, long version, byte[] value) {
        writeCount.incrementAndGet();
        data.merge(id, new VersionedValue(version, false, value), ReplicaStorage::newer);
    }

    void delete(String id, long version) {
        writeCount.incrementAndGet();
        data.merge(id, new VersionedValue(version, true, EMPTY), ReplicaStorage::newer);
    }

    long storedKeys() {
        return data.values().stream().filter(value -> !value.tombstone()).count();
    }

    long reads() {
        return readCount.get();
    }

    long writes() {
        return writeCount.get();
    }

    private static VersionedValue newer(VersionedValue current, VersionedValue candidate) {
        return candidate.version() >= current.version() ? candidate : current;
    }
}
