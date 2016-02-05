package tech.allegro.schema.json2avro.validator.schema;

import java.util.Arrays;

public enum ValidationMode {

    JSON_TO_AVRO,
    AVRO_TO_JSON,
    JSON_TO_AVRO_TO_JSON;

    public static ValidationMode from(String name) {
        return Arrays.stream(values())
            .filter(value -> value.name().replaceAll("_", "").replaceAll("TO", "2").equalsIgnoreCase(name))
            .findFirst()
            .get();
    }
}
