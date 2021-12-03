package tech.allegro.schema.json2avro.converter.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateTimeUtils {

    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("[yyyy][yy]['-']['/']['.'][' '][MMM][MM][M]['-']['/']['.'][' '][dd][d]" +
                    "[[' ']['T']HH:mm[':'ss[.][SSSSSS][SSSSS][SSSS][SSS][' '][z][zzz][Z][O][x][XXX][XX][X]]]");
    private static final DateTimeFormatter timeFormatter =
            DateTimeFormatter.ofPattern("HH:mm[':'ss[.][SSSSSS][SSSSS][SSSS][SSS]]");

    /**
     * Parse the Json date-time logical type to an Avro long value.
     * @return the number of microseconds from the unix epoch, 1 January 1970 00:00:00.000000 UTC.
     */
    public static Long getEpochMicros(String jsonDateTime) {
        Instant instant = null;
        if (jsonDateTime.matches("-?\\d+")) {
            return Long.valueOf(jsonDateTime);
        }
        try {
            ZonedDateTime zdt = ZonedDateTime.parse(jsonDateTime, formatter);
            instant = zdt.toInstant();
        } catch (DateTimeParseException e) {
            try {
                LocalDateTime dt = LocalDateTime.parse(jsonDateTime, formatter);
                instant = dt.toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException ex) {
                // no logging since it may generate too much noise
            }
        }
        return instant == null ? null : instant.toEpochMilli() * 1000;
    }

    /**
     * Parse the Json date logical type to an Avro int.
     * @return the number of days from the unix epoch, 1 January 1970 (ISO calendar).
     */
    public static Integer getEpochDay(String jsonDate) {
        Integer epochDay = null;
        try {
            LocalDate date = LocalDate.parse(jsonDate, formatter);
            epochDay = (int) date.toEpochDay();
        } catch (DateTimeParseException e) {
            // no logging since it may generate too much noise
        }
        return epochDay;
    }

    /**
     * Parse the Json time logical type to an Avro long.
     * @return the number of microseconds after midnight, 00:00:00.000000.
     */
    public static Long getMicroSeconds(String jsonTime) {
        Long nanoOfDay = null;
        if (jsonTime.matches("-?\\d+")) {
            return Long.valueOf(jsonTime);
        }
        try {
            LocalTime time = LocalTime.parse(jsonTime, timeFormatter);
            nanoOfDay = time.toNanoOfDay();
        } catch (DateTimeParseException e) {
            try {
                LocalTime time = LocalTime.parse(jsonTime, formatter);
                nanoOfDay = time.toNanoOfDay();
            } catch (DateTimeParseException ex) {
                // no logging since it may generate too much noise
            }
        }
        return nanoOfDay == null ? null : nanoOfDay / 1000;
    }
}
