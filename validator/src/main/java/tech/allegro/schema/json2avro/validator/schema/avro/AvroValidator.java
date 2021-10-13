package tech.allegro.schema.json2avro.validator.schema.avro;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;
import tech.allegro.schema.json2avro.validator.schema.ValidationMode;
import tech.allegro.schema.json2avro.validator.schema.ValidationOutput;
import tech.allegro.schema.json2avro.validator.schema.ValidationResult;
import tech.allegro.schema.json2avro.validator.schema.Validator;
import tech.allegro.schema.json2avro.validator.schema.ValidatorException;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class AvroValidator implements Validator {

    private final Logger logger = LoggerFactory.getLogger(AvroValidator.class);

    private final Schema schema;

    private final byte[] content;

    private final ValidationMode mode;

    private final ValidationOutput output;

    private final JsonAvroConverter converter;

    public AvroValidator(byte[] schema, byte[] content, ValidationMode mode, ValidationOutput output) {
        converter = new JsonAvroConverter();
        try {
            this.schema = new Schema.Parser().parse(new ByteArrayInputStream(schema));
            this.content = content;
            this.mode = mode;
            this.output = output;
        } catch (IOException e) {
            throw new ValidatorException("An unexpected error occurred when parsing the schema", e);
        }
    }

    @Override
    public ValidationResult validate() {
        byte [] result;
        switch (mode) {
            case AVRO_TO_JSON:
                result = convertAvroToJson(content);
                break;
            case JSON_TO_AVRO_TO_JSON:
                result = convertAvroToJson(convertJsonToAvro(content));
                break;
            case JSON_TO_AVRO:
            default:
                result = convertJsonToAvro(content);
                break;
        }
        return ValidationResult.success(new String(result));
    }

    private byte [] convertAvroToJson(byte [] avro) {
        try {
            logger.debug("Converting AVRO to JSON");
            byte [] json = converter.convertToJson(avro, schema);
            logger.debug("Validation result: success. JSON: \n{}", new String(json));
            output.write(json);
            return json;
        } catch (RuntimeException e) {
            throw new ValidatorException("Error occurred when validating the document", e);
        }
    }

    private byte [] convertJsonToAvro(byte [] json) {
        try {
            logger.debug("Converting JSON to AVRO");
            byte [] avro = converter.convertToAvro(json, schema);
            logger.debug("Validation result: success. AVRO: \n{}", new String(avro));
            output.write(avro);
            return avro;
        } catch (RuntimeException e) {
            throw new ValidatorException("Error occurred when validating the document", e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private byte[] input;

        private byte[] schema;

        private ValidationMode mode = ValidationMode.JSON_TO_AVRO;

        private ValidationOutput output = ValidationOutput.NO_OUTPUT;

        public Builder withSchema(byte[] schema) {
            this.schema = schema;
            return this;
        }

        public Builder withInput(byte[] input) {
            this.input = input;
            return this;
        }

        public Builder withMode(ValidationMode mode) {
            this.mode = mode;
            return this;
        }

        public Builder withOutput(ValidationOutput output) {
            this.output = output;
            return this;
        }

        public AvroValidator build() {
            return new AvroValidator(schema, input, mode, output);
        }
    }
}
