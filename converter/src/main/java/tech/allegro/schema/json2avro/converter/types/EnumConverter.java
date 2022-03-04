package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import tech.allegro.schema.json2avro.converter.PathsPrinter;

import java.util.Deque;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static tech.allegro.schema.json2avro.converter.PathsPrinter.print;

public class EnumConverter extends AvroTypeConverterWithStrictJavaTypeCheck<String> {
    public static final AvroTypeConverter INSTANCE = new EnumConverter();

    private EnumConverter() {
        super(String.class);
    }

    @Override
    public Object convertValue(Schema.Field field, Schema schema, String value, Deque<String> path, boolean silently) {
        List<String> symbols = schema.getEnumSymbols();
        if (symbols.contains(value)) {
            return new GenericData.EnumSymbol(schema, value);
        }
        throw enumException(path, symbols.stream().map(String::valueOf).collect(joining(", ")));
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> path) {
        return schema.getType().equals(Schema.Type.ENUM);
    }

    private static AvroTypeException enumException(Deque<String> fieldPath, String expectedSymbols) {
        return new AvroTypeException("Field " + print(fieldPath) + " is expected to be of enum type and be one of " + expectedSymbols);
    }
}
