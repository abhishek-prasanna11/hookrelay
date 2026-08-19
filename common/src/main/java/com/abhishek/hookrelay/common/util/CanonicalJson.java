package com.abhishek.hookrelay.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Produces a stable textual form of a JSON document so that two logically identical payloads always
 * hash to the same value.
 *
 * <p>This matters for ingest idempotency. {@code request_hash} decides whether a repeated
 * {@code Idempotency-Key} is an honest retry (same request → 200) or a producer bug (different
 * request → 409). A producer that re-serializes its payload before retrying may legitimately emit
 * the keys in a different order:
 *
 * <pre>
 *   attempt 1:  {"amount":4200,"order_id":"A-1"}
 *   attempt 2:  {"order_id":"A-1","amount":4200}
 * </pre>
 *
 * <p>These are the same event. Hashing the raw bytes would produce two different hashes and reject
 * the retry with a spurious 409 — turning our safety check into the very failure it exists to
 * prevent. Canonicalizing first removes the ambiguity.
 *
 * <p>Object keys are sorted; array order is preserved, because arrays are ordered in JSON and
 * {@code [1,2]} is genuinely not {@code [2,1]}.
 */
public final class CanonicalJson {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CanonicalJson() {
    }

    /**
     * Returns the canonical serialization: object keys sorted recursively, no insignificant
     * whitespace.
     */
    public static String canonicalize(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(sorted(node));
        } catch (JsonProcessingException e) {
            // sorted() only ever rebuilds nodes that Jackson already parsed, so this is unreachable
            // in practice; rethrowing keeps the method total rather than swallowing a real bug.
            throw new IllegalStateException("failed to serialize canonical JSON", e);
        }
    }

    private static JsonNode sorted(JsonNode node) {
        if (node.isObject()) {
            ObjectNode source = (ObjectNode) node;
            List<String> fieldNames = new ArrayList<>();
            for (Iterator<String> it = source.fieldNames(); it.hasNext(); ) {
                fieldNames.add(it.next());
            }
            Collections.sort(fieldNames);

            ObjectNode result = MAPPER.createObjectNode();
            for (String name : fieldNames) {
                result.set(name, sorted(source.get(name)));
            }
            return result;
        }

        if (node.isArray()) {
            ArrayNode result = MAPPER.createArrayNode();
            for (JsonNode element : node) {
                result.add(sorted(element));
            }
            return result;
        }

        return node;
    }
}
