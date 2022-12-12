package tech.allegro.schema.json2avro.converter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.allegro.schema.json2avro.converter.util.DateTimeUtils.getEpochDay;
import static tech.allegro.schema.json2avro.converter.util.DateTimeUtils.getEpochMicros;
import static tech.allegro.schema.json2avro.converter.util.DateTimeUtils.getMicroSeconds;

public class DateTimeUtilsTest {

    @Test
    public void testDateTimeConversion() {

        assertEquals(1537012800000000L, getEpochMicros("2018-09-15 12:00:00"));
        assertEquals(1537012800006000L, getEpochMicros("2018-09-15 12:00:00.006542"));
        assertEquals(1537012800000000L, getEpochMicros("2018/09/15 12:00:00"));
        assertEquals(1537012800000000L, getEpochMicros("2018.09.15 12:00:00"));
        assertEquals(1531656000000000L, getEpochMicros("2018 Jul 15 12:00:00"));
        assertEquals(1531627200000000L, getEpochMicros("2018 Jul 15 12:00:00 GMT+08:00"));
        assertEquals(1531630800000000L, getEpochMicros("2018 Jul 15 12:00:00GMT+07"));
        assertEquals(1609462861000000L, getEpochMicros("2021-1-1 01:01:01"));
        assertEquals(1609462861000000L, getEpochMicros("2021.1.1 01:01:01"));
        assertEquals(1609462861000000L, getEpochMicros("2021/1/1 01:01:01"));
        assertEquals(1609459261000000L, getEpochMicros("2021-1-1 01:01:01 +01"));
        assertEquals(1609459261000000L, getEpochMicros("2021-01-01T01:01:01+01:00"));
        assertEquals(1609459261546000L, getEpochMicros("2021-01-01T01:01:01.546+01:00"));
        assertEquals(1609462861000000L, getEpochMicros("2021-01-01 01:01:01"));
        assertEquals(1609462861000000L, getEpochMicros("2021-01-01 01:01:01 +0000"));
        assertEquals(1609462861000000L, getEpochMicros("2021/01/01 01:01:01 +0000"));
        assertEquals(1609462861000000L, getEpochMicros("2021-01-01T01:01:01Z"));
        assertEquals(1609466461000000L, getEpochMicros("2021-01-01T01:01:01-01:00"));
        assertEquals(1609459261000000L, getEpochMicros("2021-01-01T01:01:01+01:00"));
        assertEquals(1609462861000000L, getEpochMicros("2021-01-01 01:01:01 UTC"));
        assertEquals(1609491661000000L, getEpochMicros("2021-01-01T01:01:01 PST"));
        assertEquals(1609462861000000L, getEpochMicros("2021-01-01T01:01:01 +0000"));
        assertEquals(1609462861000000L, getEpochMicros("2021-01-01T01:01:01+0000"));
        assertEquals(1609462861000000L, getEpochMicros("2021-01-01T01:01:01UTC"));
        assertEquals(1609459261000000L, getEpochMicros("2021-01-01T01:01:01+01"));
        assertEquals(-125941863974322000L, getEpochMicros("2022-01-23T01:23:45.678-11:30 BC"));
        assertEquals(1642942425678000L, getEpochMicros("2022-01-23T01:23:45.678-11:30"));

        assertEquals(18628, getEpochDay("2021-1-1"));
        assertEquals(18628, getEpochDay("2021-01-01"));
        assertEquals(18629, getEpochDay("2021/01/02"));
        assertEquals(18630, getEpochDay("2021.01.03"));
        assertEquals(18631, getEpochDay("2021 Jan 04"));
        assertEquals(-1457318, getEpochDay("2021-1-1 BC"));

        assertEquals(3661000000L, getMicroSeconds("01:01:01"));
        assertEquals(3660000000L, getMicroSeconds("01:01"));
        assertEquals(44581541000L, getMicroSeconds("12:23:01.541"));
        assertEquals(44581541214L, getMicroSeconds("12:23:01.541214"));
    }

    @Test
    public void cleaNLineBreaksTest() {
        assertEquals(1585612800000000L, getEpochMicros("2020-03-\n31T00:00:00Z\r"));
        assertEquals(18628, getEpochDay("2021-\n1-1\r"));
    }
}
