package com.abhishek.hookrelay.common.util;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * Generator for UUID version 7 (RFC 9562): a 48-bit Unix millisecond timestamp in the high bits,
 * followed by random data.
 *
 * <p>Why not {@link UUID#randomUUID()} (version 4)? Both are equally unguessable, but a v4 value is
 * random across its whole range, so inserting v4 primary keys scatters writes uniformly across the
 * B-tree index. Every insert dirties a different page, the whole index has to stay resident to
 * avoid random reads, and pages split constantly. A v7 value is time-ordered, so consecutive
 * inserts land in the same right-most page — the access pattern of a sequence — while keeping the
 * unpredictability we need from {@code deliveries.id}, which is published to customers as the
 * {@code X-HookRelay-Delivery-Id} header.
 *
 * <p>Layout (RFC 9562 section 5.7):
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                          unix_ts_ms                           |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |          unix_ts_ms           |  ver  |       rand_a          |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |var|                        rand_b                             |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                            rand_b                             |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * </pre>
 *
 * <p>Java 21 has no built-in v7 generator, hence this class.
 */
public final class Uuid7 {

    private static final SecureRandom RANDOM = new SecureRandom();

    /** Mask for the low 48 bits, the width of the millisecond timestamp field. */
    private static final long TIMESTAMP_MASK = 0xFFFF_FFFF_FFFFL;

    /** Version 7, positioned at bits 12-15 of the most-significant long. */
    private static final long VERSION_7 = 0x7000L;

    /** Mask for rand_a, the 12 bits following the version nibble. */
    private static final long RAND_A_MASK = 0x0FFFL;

    /** Mask clearing the top 2 bits of the least-significant long, where the variant lives. */
    private static final long RAND_B_MASK = 0x3FFF_FFFF_FFFF_FFFFL;

    /** RFC 9562 variant bits {@code 10}, occupying the top 2 bits of the least-significant long. */
    private static final long VARIANT_RFC9562 = 0x8000_0000_0000_0000L;

    private Uuid7() {
    }

    public static UUID generate() {
        return generate(System.currentTimeMillis());
    }

    /** Package-visible for tests that need a fixed timestamp to assert ordering. */
    static UUID generate(long unixMillis) {
        long randA = RANDOM.nextInt(1 << 12);

        long msb = (unixMillis & TIMESTAMP_MASK) << 16
                | VERSION_7
                | (randA & RAND_A_MASK);

        long lsb = (RANDOM.nextLong() & RAND_B_MASK) | VARIANT_RFC9562;

        return new UUID(msb, lsb);
    }

    /** Extracts the embedded creation timestamp. Useful for debugging and for tests. */
    public static long timestampOf(UUID uuid) {
        if (uuid.version() != 7) {
            throw new IllegalArgumentException("not a UUIDv7: " + uuid);
        }
        return uuid.getMostSignificantBits() >>> 16;
    }
}
