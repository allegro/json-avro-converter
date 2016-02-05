package tech.allegro.schema.json2avro.validator.schema;


import tech.allegro.schema.json2avro.validator.schema.avro.AvroValidator;

public interface Validators {

    static AvroValidator.Builder avro() {
        return AvroValidator.builder();
    }
}
