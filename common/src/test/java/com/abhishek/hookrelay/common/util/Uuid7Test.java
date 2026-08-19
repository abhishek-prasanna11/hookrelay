package com.abhishek.hookrelay.common.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class Uuid7Test {

    @Test
    @DisplayName("generated values carry version 7 and the RFC 9562 variant")
    void hasCorrectVersionAndVariant() {
        UUID uuid = Uuid7.generate();

        assertThat(uuid.version()).isEqualTo(7);
        assertThat(uuid.variant()).isEqualTo(2);   // binary 10
    }

    @Test
    @DisplayName("the embedded timestamp round-trips")
    void embedsTimestamp() {
        long millis = 1_700_000_000_000L;

        assertThat(Uuid7.timestampOf(Uuid7.generate(millis))).isEqualTo(millis);
    }

    @Test
    @DisplayName("values from increasing timestamps sort in timestamp order")
    void isTimeOrdered() {
        List<UUID> generated = new ArrayList<>();
        for (long millis = 1_700_000_000_000L; millis < 1_700_000_000_100L; millis++) {
            generated.add(Uuid7.generate(millis));
        }

        List<UUID> sorted = new ArrayList<>(generated);
        sorted.sort((a, b) -> Long.compareUnsigned(a.getMostSignificantBits(), b.getMostSignificantBits()));

        // This ordering property is the whole reason for using v7: it is what makes primary-key
        // inserts append to the right edge of the index instead of scattering across it.
        assertThat(sorted).isEqualTo(generated);
    }

    @Test
    @DisplayName("values are unique within the same millisecond")
    void isUniqueWithinOneMillisecond() {
        Set<UUID> values = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            values.add(Uuid7.generate(1_700_000_000_000L));
        }

        assertThat(values).hasSize(10_000);
    }

    @Test
    @DisplayName("timestampOf refuses a non-v7 UUID rather than returning nonsense")
    void rejectsNonV7() {
        UUID v4 = UUID.randomUUID();

        assertThat(v4.version()).isEqualTo(4);
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class, () -> Uuid7.timestampOf(v4));
    }
}
