package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Deque;

import static org.apache.avro.Schema.Type.INT;
import static tech.allegro.schema.json2avro.converter.PathsPrinter.print;
import static tech.allegro.schema.json2avro.converter.types.AvroTypeConverter.isLogicalType;

public class IntDateConverter implements AvroTypeConverter {
    public static final AvroTypeConverter INSTANCE = new IntDateConverter(DateTimeFormatter.ISO_DATE);
    public static final String VALID_JSON_FORMAT = "date string, epoch days number";

    private final DateTimeFormatter dateTimeFormatter;

    public IntDateConverter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        if (jsonValue instanceof String) {
            String dateString = (String) jsonValue;
            try {
                return LocalDate.from(dateTimeFormatter.parse(dateString)).toEpochDay();
            } catch (DateTimeParseException exception) {
                if (silently) {
                    return new AvroTypeConverter.Incompatible(VALID_JSON_FORMAT);
                } else {
                    throw new AvroTypeException("Field " + print(path) + " should be a valid date.");
                }
            }
        } else if (jsonValue instanceof Number) {
            return ((Number) jsonValue).intValue();
        }

        if (silently) {
            return new AvroTypeConverter.Incompatible(VALID_JSON_FORMAT);
        } else {
            throw new AvroTypeException("Field " + print(path) + " is expected to be type: java.lang.String or java.lang.Number.");
        }
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> deque) {
        return INT.equals(schema.getType()) && isLogicalType(schema, LogicalTypes.date().getName());
    }
}
