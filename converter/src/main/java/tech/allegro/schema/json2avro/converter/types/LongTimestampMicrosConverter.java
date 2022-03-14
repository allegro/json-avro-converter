package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Deque;

import static org.apache.avro.Schema.Type.LONG;
import static tech.allegro.schema.json2avro.converter.PathsPrinter.print;
import static tech.allegro.schema.json2avro.converter.types.AvroTypeConverter.isLogicalType;

public class LongTimestampMicrosConverter implements AvroTypeConverter {
    public static final AvroTypeConverter INSTANCE = new LongTimestampMicrosConverter(DateTimeFormatter.ISO_DATE_TIME);
    public static final String VALID_JSON_FORMAT = "date time string, timestamp number";

    private final DateTimeFormatter dateTimeFormatter;

    public LongTimestampMicrosConverter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        if (jsonValue instanceof String) {
            String dateString = (String) jsonValue;
            try {
                Instant instant = Instant.from(dateTimeFormatter.parse(dateString));
                // based on org.apache.avro.data.TimestampMicrosConversion
                long seconds = instant.getEpochSecond();
                int nanos = instant.getNano();

                if (seconds < 0 && nanos > 0) {
                    long micros = Math.multiplyExact(seconds + 1, 1_000_000);
                    long adjustment = (nanos / 1_000L) - 1_000_000;

                    return Math.addExact(micros, adjustment);
                } else {
                    long micros = Math.multiplyExact(seconds, 1_000_000);

                    return Math.addExact(micros, nanos / 1_000);
                }
            } catch (DateTimeParseException exception) {
                if (silently) {
                    return new AvroTypeConverter.Incompatible(VALID_JSON_FORMAT);
                } else {
                    throw new AvroTypeException("Field " + print(path) + " should be a valid date time.");
                }
            }
        } else if (jsonValue instanceof Number) {
            return ((Number) jsonValue).longValue();
        }

        if (silently) {
            return new AvroTypeConverter.Incompatible(VALID_JSON_FORMAT);
        } else {
            throw new AvroTypeException("Field " + print(path) + " is expected to be type: java.lang.String or java.lang.Number.");
        }
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> deque) {
        return LONG.equals(schema.getType()) && isLogicalType(schema, LogicalTypes.timestampMicros().getName());
    }
}
