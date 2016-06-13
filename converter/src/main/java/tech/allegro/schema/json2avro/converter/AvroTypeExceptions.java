package tech.allegro.schema.json2avro.converter;

import org.apache.avro.AvroTypeException;

import java.util.Deque;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.joining;

class AvroTypeExceptions {
    static AvroTypeException enumException(Deque<String> fieldPath, String expectedSymbols) {
        return new AvroTypeException(new StringBuilder()
                .append("Field ")
                .append(path(fieldPath))
                .append(" is expected to be of enum type and be one of ")
                .append(expectedSymbols)
                .toString());
    }

    static AvroTypeException unionException(String fieldName, String expectedTypes, Deque<String> offendingPath) {
        return new AvroTypeException(new StringBuilder()
                .append("Could not evaluate union, field")
                .append(fieldName)
                .append("is expected to be one of these: ")
                .append(expectedTypes)
                .append("If this is a complex type, check if offending field: ")
                .append(path(offendingPath))
                .append(" adheres to schema.")
                .toString());
    }

    static AvroTypeException typeException(Deque<String> fieldPath, String expectedType) {
        return new AvroTypeException(new StringBuilder()
            .append("Field ")
            .append(path(fieldPath))
            .append(" is expected to be type: ")
            .append(expectedType)
            .toString());
    }

    private static String path(Deque<String> path) {
        return StreamSupport.stream(spliteratorUnknownSize(path.descendingIterator(), ORDERED), false)
                .map(Object::toString).collect(joining("."));
    }
}
