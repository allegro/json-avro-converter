package tech.allegro.schema.json2avro.validator.schema;

public interface ValidationOutput {

    ValidationOutput NO_OUTPUT = output -> {};

    void write(byte[] output);
}
