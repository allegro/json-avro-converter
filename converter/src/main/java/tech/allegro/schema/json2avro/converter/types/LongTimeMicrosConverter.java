package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

import static org.apache.avro.Schema.Type.LONG;
import static tech.allegro.schema.json2avro.converter.PathsPrinter.print;
import static tech.allegro.schema.json2avro.converter.types.AvroTypeConverter.isLogicalType;

public class LongTimeMicrosConverter implements AvroTypeConverter {
    public static final AvroTypeConverter INSTANCE = new LongTimeMicrosConverter(DateTimeFormatter.ISO_TIME);
    public static final String VALID_JSON_FORMAT = "time string, micros number";

    private final DateTimeFormatter dateTimeFormatter;

    public LongTimeMicrosConverter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        if (jsonValue instanceof String) {
            String dateString = (String) jsonValue;
            try {
                long nanoOfDay = LocalTime.from(dateTimeFormatter.parse(dateString)).toNanoOfDay();
                return TimeUnit.NANOSECONDS.toMicros(nanoOfDay);
            } catch (DateTimeParseException exception) {
                if (silently) {
                    return new Incompatible(VALID_JSON_FORMAT);
                } else {
                    throw new AvroTypeException("Field " + print(path) + " should be a valid time.");
                }
            }
        } else if (jsonValue instanceof Number) {
            return ((Number) jsonValue).longValue();
        }

        if (silently) {
            return new Incompatible(VALID_JSON_FORMAT);
        } else {
            throw new AvroTypeException("Field " + print(path) + " is expected to be type: java.lang.String or java.lang.Number.");
        }
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> deque) {
        return LONG.equals(schema.getType()) && isLogicalType(schema, LogicalTypes.timeMicros().getName());
    }
}
