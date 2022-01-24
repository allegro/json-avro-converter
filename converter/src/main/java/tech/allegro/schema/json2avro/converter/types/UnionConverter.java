package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import tech.allegro.schema.json2avro.converter.JsonToAvroReader;
import tech.allegro.schema.json2avro.converter.PathsPrinter;

import java.util.Deque;
import java.util.List;

import static java.util.stream.Collectors.joining;

public class UnionConverter implements AvroTypeConverter {
    private final JsonToAvroReader jsonToAvroReader;

    public UnionConverter(JsonToAvroReader jsonToAvroReader) {
        this.jsonToAvroReader = jsonToAvroReader;
    }

    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        List<Schema> types = schema.getTypes();
        for (Schema type : types) {
            try {
                Object nestedValue = this.jsonToAvroReader.read(field, type, jsonValue, path, true);
                if (nestedValue == INCOMPATIBLE) {
                    continue;
                } else {
                    return nestedValue;
                }
            } catch (AvroRuntimeException e) {
                // thrown only for union of more complex types like records
                continue;
            }
        }
        throw unionException(
                field.name(),
                types.stream().map(Schema::getType).map(Object::toString).collect(joining(", ")),
                path);
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> path) {
        return schema.getType().equals(Schema.Type.UNION);
    }

    private static AvroTypeException unionException(String fieldName, String expectedTypes, Deque<String> offendingPath) {
        return new AvroTypeException(new StringBuilder()
                .append("Could not evaluate union, field ")
                .append(fieldName)
                .append(" is expected to be one of these: ")
                .append(expectedTypes)
                .append(". If this is a complex type, check if offending field: ")
                .append(PathsPrinter.print(offendingPath))
                .append(" adheres to schema.")
                .toString());
    }
}
