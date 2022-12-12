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

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("[yyyy][yy]['-']['/']['.'][' '][MMM][MM][M]['-']['/']['.'][' '][dd][d][[' '][G]][[' ']['T']HH:mm[':'ss[.][SSSSSS][SSSSS][SSSS][SSS][' '][z][zzz][Z][O][x][XXX][XX][X][[' '][G]]]]");
    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm[':'ss[.][SSSSSS][SSSSS][SSSS][SSS][' '][z][zzz][Z][O][x][XXX][XX][X]]");

    /**
     * Parse the Json date-time logical type to an Avro long value.
     * @return the number of microseconds from the unix epoch, 1 January 1970 00:00:00.000000 UTC.
     */
    public static Long getEpochMicros(String jsonDateTime) {
        jsonDateTime = cleanLineBreaks(jsonDateTime);
        Instant instant = null;
        if (jsonDateTime.matches("-?\\d+")) {
            return Long.valueOf(jsonDateTime);
        }
        try {
            ZonedDateTime zdt = ZonedDateTime.parse(jsonDateTime, DATE_TIME_FORMATTER);
            instant = zdt.toInstant();
        } catch (DateTimeParseException e) {
            try {
                LocalDateTime dt = LocalDateTime.parse(jsonDateTime, DATE_TIME_FORMATTER);
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
        jsonDate = cleanLineBreaks(jsonDate);
        Integer epochDay = null;
        try {
            LocalDate date = LocalDate.parse(jsonDate, DATE_TIME_FORMATTER);
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
        jsonTime = cleanLineBreaks(jsonTime);
        Long nanoOfDay = null;
        if (jsonTime.matches("-?\\d+")) {
            return Long.valueOf(jsonTime);
        }
        try {
            LocalTime time = LocalTime.parse(jsonTime, TIME_FORMATTER);
            nanoOfDay = time.toNanoOfDay();
        } catch (DateTimeParseException e) {
            try {
                LocalTime time = LocalTime.parse(jsonTime, DATE_TIME_FORMATTER);
                nanoOfDay = time.toNanoOfDay();
            } catch (DateTimeParseException ex) {
                // no logging since it may generate too much noise
            }
        }
        return nanoOfDay == null ? null : nanoOfDay / 1000;
    }

    private static String cleanLineBreaks(String jsonDateTime) {
        return jsonDateTime.replace("\n", "").replace("\r", "");
    }
}
