package tech.allegro.schema.json2avro.converter;

import java.util.Deque;
import org.apache.avro.AvroTypeException;

class AvroTypeExceptions {

    static AvroTypeException enumException(Deque<String> fieldPath, String expectedSymbols, Object offendingValue) {
        return new AvroTypeException(
            String.format("Field %s is expected to be of enum type and be one of %s, but it is: %s",
                PathsPrinter.print(fieldPath), expectedSymbols, offendingValue));
    }

    static AvroTypeException unionException(String fieldName, String expectedTypes, Deque<String> offendingPath, Object offendingValue) {
        return new AvroTypeException(
            String.format("Could not evaluate union, field %s is expected to be one of these: %s. "
                    + "If this is a complex type, check if offending field (path: %s) adheres to schema: %s",
                fieldName, expectedTypes, PathsPrinter.print(offendingPath), offendingValue));
    }

    static AvroTypeException typeException(Deque<String> fieldPath, String expectedType, Object offendingValue) {
        return new AvroTypeException(
            String.format("Field %s is expected to be type: %s, but it is: %s", PathsPrinter.print(fieldPath), expectedType, offendingValue));
    }

    static AvroTypeException numberFormatException(Deque<String> fieldPath, Object offendingValue) {
        return new AvroTypeException(
            String.format("Field %s is expected to be Number format, but it is: %s", PathsPrinter.print(fieldPath), offendingValue));
    }
}
