package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class IntTimeMillisConverter extends AbstractIntDateTimeConverter {
    public static final AvroTypeConverter INSTANCE = new IntTimeMillisConverter(DateTimeFormatter.ISO_TIME);

    private final DateTimeFormatter dateTimeFormatter;

    public IntTimeMillisConverter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    protected Object convertDateTimeString(String dateTimeString) {
        long nanoOfDay = parseLocalTime(dateTimeString).toNanoOfDay();
        return TimeUnit.NANOSECONDS.toMillis(nanoOfDay);
    }

    protected LocalTime parseLocalTime(String dateTimeString) {
        return LocalTime.from(dateTimeFormatter.parse(dateTimeString));
    }

    @Override
    protected LogicalType getLogicalType() {
        return LogicalTypes.timeMillis();
    }

    @Override
    protected String getValidStringFormat() {
        return "time";
    }

    @Override
    protected String getValidNumberFormat() {
        return "millis";
    }
}
