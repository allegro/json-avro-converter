package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class IntDateConverter extends AbstractIntDateTimeConverter {
    public static final AvroTypeConverter INSTANCE = new IntDateConverter(DateTimeFormatter.ISO_DATE);

    private final DateTimeFormatter dateTimeFormatter;

    public IntDateConverter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    protected Object parseDateTime(String dateTimeString) {
        return LocalDate.from(dateTimeFormatter.parse(dateTimeString)).toEpochDay();
    }

    @Override
    protected LogicalType getLogicalType() {
        return LogicalTypes.date();
    }

    @Override
    protected String getValidStringFormat() {
        return "date";
    }

    @Override
    protected String getValidNumberFormat() {
        return "epoch days";
    }
}
