package tech.allegro.schema.json2avro.converter;

import java.util.Deque;
import org.apache.avro.AvroTypeException;

class AvroTypeExceptions {
    static AvroTypeException enumException(Deque<String> fieldPath, String expectedSymbols, Object offendingValue) {
        return new AvroTypeException(new StringBuilder()
                .append("Field ")
                .append(PathsPrinter.print(fieldPath))
                .append(" is expected to be of enum type and be one of ")
                .append(expectedSymbols)
                .append(", but it is: ")
                .append(offendingValue)
                .toString());
    }

    static AvroTypeException unionException(String fieldName, String expectedTypes, Deque<String> offendingPath, Object offendingValue) {
        return new AvroTypeException(new StringBuilder()
                .append("Could not evaluate union, field ")
                .append(fieldName)
                .append(" is expected to be one of these: ")
                .append(expectedTypes)
                .append(". If this is a complex type, check if offending field (path: ")
                .append(PathsPrinter.print(offendingPath))
                .append(") adheres to schema: ")
                .append(offendingValue)
                .toString());
    }

    static AvroTypeException typeException(Deque<String> fieldPath, String expectedType, Object offendingValue) {
        return new AvroTypeException(new StringBuilder()
            .append("Field ")
            .append(PathsPrinter.print(fieldPath))
            .append(" is expected to be type: ")
            .append(expectedType)
            .append(", but it is: ")
            .append(offendingValue)
            .toString());
    }
}
