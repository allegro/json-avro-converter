package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import tech.allegro.schema.json2avro.converter.PathsPrinter;

import java.util.Deque;

import static tech.allegro.schema.json2avro.converter.PathsPrinter.print;

public abstract class AvroTypeConverterWithStrictJavaTypeCheck<T> implements AvroTypeConverter {
    private final Class<T> javaType;

    protected AvroTypeConverterWithStrictJavaTypeCheck(Class<T> javaType) {
        this.javaType = javaType;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        if (javaType.isInstance(jsonValue)) {
            return this.convertValue(field, schema, (T) jsonValue, path, silently);
        } else {
            if (silently) {
                return new Incompatible(this.javaType.getTypeName());
            } else {
                throw typeException(path, javaType.getTypeName());
            }
        }
    }

    public abstract Object convertValue(Schema.Field field, Schema schema, T value, Deque<String> path, boolean silently);

    private static AvroTypeException typeException(Deque<String> fieldPath, String expectedType) {
        return new AvroTypeException("Field " + print(fieldPath) + " is expected to be type: " + expectedType);
    }
}
