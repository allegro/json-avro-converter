package tech.allegro.schema.json2avro.converter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import java.time.LocalDateTime
import java.time.ZoneOffset

class LongTimestampMillisConverterSpec extends BaseConverterSpec {

    def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "datetime",
                    "type" : {
                      "type" : "long",
                      "logicalType" : "timestamp-millis"
                    }
                  }
              ]
            }
        '''

    def schemaWithNullableTimestamp = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "datetime",
                    "type" : [{
                      "type" : "long",
                      "logicalType" : "timestamp-millis"
                    }, "null"]
                  }
              ]
            }
        '''

    def "should convert iso date time to timestamp-millis"() {
        given:
        def json = '''
        {
            "datetime": "2022-02-05T16:20:29Z"
        }
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        LocalDateTime.of(2022, 02, 05, 16, 20, 29).toEpochSecond(ZoneOffset.UTC) * 1000 == record.get("datetime")
    }

    def "should convert iso date time to timestamp-millis in millis precision"() {
        given:
        def json = '''
        {
            "datetime": "2022-02-05T16:20:29.234Z"
        }
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        LocalDateTime.of(2022, 02, 05, 16, 20, 29, 234 * 1000 * 1000).toInstant(ZoneOffset.UTC).toEpochMilli() == record.get("datetime")
    }

    def "should convert long time to timestamp-millis"() {
        given:
        def json = '''
        {
            "datetime": 1644078029000
        }
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        1644078029000 == record.get("datetime")
    }

    def "should fail if date time is malformed"() {
        given:
        def json = '''
        {
            "datetime": "test"
        }
        '''

        when:
        converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Field datetime should be a valid date time."
    }

    def "should fail if date is not a String nor a Long"() {
        given:
        def json = '''
        {
            "datetime": {}
        }
        '''

        when:
        converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Field datetime is expected to be type: java.lang.String or java.lang.Number."
    }

    def "should allow a timestamp-millis to be nullable"() {
        given:
        def json = '''
        {
            "datetime": null
        }
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schemaWithNullableTimestamp))

        then:
        null == record.get("datetime")
    }

    def "should throw error when timestamp-millis malformed on union type"() {
        given:
        def json = '''
        {
            "datetime": "test"
        }
        '''

        when:
        converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schemaWithNullableTimestamp))

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Could not evaluate union, field datetime is expected to be one of these: date time string, timestamp number, NULL. If this is a complex type, check if offending field: datetime adheres to schema."
    }

}
