package com.abhishek.hookrelay.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CanonicalJsonTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static String canonical(String json) throws Exception {
        return CanonicalJson.canonicalize(MAPPER.readTree(json));
    }

    @Test
    @DisplayName("object keys are sorted")
    void sortsObjectKeys() throws Exception {
        assertThat(canonical("""
                {"b": 1, "a": 2}""")).isEqualTo("{\"a\":2,\"b\":1}");
    }

    @Test
    @DisplayName("nested objects are sorted recursively")
    void sortsNestedObjects() throws Exception {
        String one = canonical("""
                {"outer": {"z": 1, "a": {"y": 2, "b": 3}}}""");
        String two = canonical("""
                {"outer": {"a": {"b": 3, "y": 2}, "z": 1}}""");

        assertThat(one).isEqualTo(two);
    }

    @Test
    @DisplayName("insignificant whitespace is removed")
    void stripsWhitespace() throws Exception {
        assertThat(canonical("""
                {  "a"  :  1  }""")).isEqualTo("{\"a\":1}");
    }

    @Test
    @DisplayName("array order is preserved — [1,2] is genuinely not [2,1]")
    void preservesArrayOrder() throws Exception {
        assertThat(canonical("[1,2]")).isNotEqualTo(canonical("[2,1]"));
        assertThat(canonical("[1,2]")).isEqualTo("[1,2]");
    }

    @Test
    @DisplayName("objects inside arrays are sorted, without reordering the array")
    void sortsObjectsInsideArrays() throws Exception {
        assertThat(canonical("""
                [{"b":1,"a":2},{"d":3,"c":4}]"""))
                .isEqualTo("[{\"a\":2,\"b\":1},{\"c\":4,\"d\":3}]");
    }

    @Test
    @DisplayName("scalars and null pass through unchanged")
    void handlesScalars() throws Exception {
        assertThat(canonical("null")).isEqualTo("null");
        assertThat(canonical("42")).isEqualTo("42");
        assertThat(canonical("\"text\"")).isEqualTo("\"text\"");
        assertThat(canonical("true")).isEqualTo("true");
    }

    @Test
    @DisplayName("an empty object is stable")
    void handlesEmptyObject() throws Exception {
        assertThat(canonical("{}")).isEqualTo("{}");
    }
}
