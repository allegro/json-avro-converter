package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class LongTimeMicrosConverter extends AbstractLongDateTimeConverter {
    public static final AvroTypeConverter INSTANCE = new LongTimeMicrosConverter(DateTimeFormatter.ISO_TIME);

    private final DateTimeFormatter dateTimeFormatter;

    public LongTimeMicrosConverter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    protected Object convertDateTimeString(String dateTimeString) {
        long nanoOfDay = parseLocalTime(dateTimeString).toNanoOfDay();
        return TimeUnit.NANOSECONDS.toMicros(nanoOfDay);
    }

    protected LocalTime parseLocalTime(String dateTimeString) {
        return LocalTime.from(dateTimeFormatter.parse(dateTimeString));
    }

    @Override
    protected LogicalType getLogicalType() {
        return LogicalTypes.timeMicros();
    }

    @Override
    protected String getValidStringFormat() {
        return "time";
    }

    @Override
    protected String getValidNumberFormat() {
        return "micros";
    }

}
