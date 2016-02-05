package tech.allegro.schema.json2avro.validator.schema;

public class ValidatorException extends RuntimeException {

    public ValidatorException(String message) {
        super(message);
    }

    public ValidatorException(String message, Throwable cause) {
        super(message, cause);
    }
}
