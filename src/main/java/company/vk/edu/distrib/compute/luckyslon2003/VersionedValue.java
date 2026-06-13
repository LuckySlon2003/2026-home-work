package company.vk.edu.distrib.compute.luckyslon2003;

// Значение с версией. Версия монотонно назначается координатором, что позволяет
// репликам и читателю детерминированно выбирать самую свежую запись по ключу.
final class VersionedValue {

    private final long revision;
    private final boolean deleted;
    private final byte[] payload;

    VersionedValue(long version, boolean tombstone, byte[] value) {
        this.revision = version;
        this.deleted = tombstone;
        this.payload = value.clone();
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
