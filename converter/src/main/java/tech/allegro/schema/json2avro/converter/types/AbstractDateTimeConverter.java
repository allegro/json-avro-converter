package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.time.format.DateTimeParseException;
import java.util.Deque;

import static tech.allegro.schema.json2avro.converter.PathsPrinter.print;

public abstract class AbstractDateTimeConverter implements AvroTypeConverter {

    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        if (jsonValue instanceof String) {
            String dateTimeString = (String) jsonValue;
            try {
                return parseDateTime(dateTimeString);
            } catch (DateTimeParseException exception) {
                if (silently) {
                    return new Incompatible(getValidJsonFormat());
                } else {
                    throw new AvroTypeException("Field " + print(path) + " should be a valid " + getValidStringFormat() + ".");
                }
            }
        } else if (jsonValue instanceof Number) {
            return toTargetNumberFormat((Number) jsonValue);
        }

        if (silently) {
            return new Incompatible(getValidJsonFormat());
        } else {
            throw new AvroTypeException("Field " + print(path) + " is expected to be type: java.lang.String or java.lang.Number.");
        }
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> path) {
        return getUnderlyingSchemaType().equals(schema.getType()) && AvroTypeConverter.isLogicalType(schema, getLogicalType().getName());
    }

    protected abstract Object parseDateTime(String dateTimeString);

    protected abstract Object toTargetNumberFormat(Number numberValue);

    protected abstract Schema.Type getUnderlyingSchemaType();

    protected abstract LogicalType getLogicalType();

    private String getValidJsonFormat() {
        return getValidStringFormat() + " string, " + getValidNumberFormat() + " number";
    }

    protected abstract String getValidStringFormat();

    protected abstract String getValidNumberFormat();

}
