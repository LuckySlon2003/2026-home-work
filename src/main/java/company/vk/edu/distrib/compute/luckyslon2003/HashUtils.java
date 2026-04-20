package company.vk.edu.distrib.compute.luckyslon2003;

import java.nio.charset.StandardCharsets;

final class HashUtils {
    private static final long FNV_64_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_64_PRIME = 0x100000001b3L;

    private HashUtils() {
    }

    static long hash64(String value) {
        long hash = FNV_64_OFFSET_BASIS;
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        for (byte currentByte : bytes) {
            hash ^= currentByte & 0xffL;
            hash *= FNV_64_PRIME;
        }

        hash ^= hash >>> 32;
        hash *= 0xd6e8feb86659fd93L;
        hash ^= hash >>> 32;
        return hash & Long.MAX_VALUE;
    }
}
