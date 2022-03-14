package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

import static org.apache.avro.Schema.Type.INT;
import static tech.allegro.schema.json2avro.converter.PathsPrinter.print;
import static tech.allegro.schema.json2avro.converter.types.AvroTypeConverter.isLogicalType;

public class IntTimeMillisConverter implements AvroTypeConverter {
    public static final AvroTypeConverter INSTANCE = new IntTimeMillisConverter(DateTimeFormatter.ISO_TIME);
    public static final String VALID_JSON_FORMAT = "time string, millis number";

    private final DateTimeFormatter dateTimeFormatter;

    public IntTimeMillisConverter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        if (jsonValue instanceof String) {
            String dateString = (String) jsonValue;
            try {
                long nanoOfDay = LocalTime.from(dateTimeFormatter.parse(dateString)).toNanoOfDay();
                return TimeUnit.NANOSECONDS.toMillis(nanoOfDay);
            } catch (DateTimeParseException exception) {
                if (silently) {
                    return new Incompatible(VALID_JSON_FORMAT);
                } else {
                    throw new AvroTypeException("Field " + print(path) + " should be a valid time.");
                }
            }
        } else if (jsonValue instanceof Number) {
            return ((Number) jsonValue).intValue();
        }

        if (silently) {
            return new Incompatible(VALID_JSON_FORMAT);
        } else {
            throw new AvroTypeException("Field " + print(path) + " is expected to be type: java.lang.String or java.lang.Number.");
        }
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> deque) {
        return INT.equals(schema.getType()) && isLogicalType(schema, LogicalTypes.timeMillis().getName());
    }
}
