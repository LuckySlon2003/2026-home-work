package company.vk.edu.distrib.compute.luckyslon2003;

// Значение с версией. Версия монотонно назначается координатором, что позволяет
// репликам и читателю детерминированно выбирать самую свежую запись по ключу.
final class VersionedValue {

    private final long version;
    private final boolean tombstone;
    private final byte[] value;

    VersionedValue(long version, boolean tombstone, byte[] value) {
        this.version = version;
        this.tombstone = tombstone;
        this.value = value.clone();
    }

    long version() {
        return version;
    }

    boolean tombstone() {
        return tombstone;
    }

    byte[] value() {
        return value.clone();
    }
}
