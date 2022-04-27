package tech.allegro.schema.json2avro.converter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import java.time.LocalDateTime
import java.time.ZoneOffset

class LongTimestampMicrosConverterSpec extends BaseConverterSpec {

    def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "datetime",
                    "type" : {
                      "type" : "long",
                      "logicalType" : "timestamp-micros"
                    }
                  }
              ]
            }
        '''

    def "should convert iso date time to timestamp-micros"() {
        given:
        def json = '''
        {
            "datetime": "2022-02-05T16:20:29.123456Z"
        }
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        def expectedInstant = LocalDateTime.of(2022, 02, 05, 16, 20, 29, 123456 * 1000).toInstant(ZoneOffset.UTC)
        expectedInstant.getEpochSecond() * 1000_000 + expectedInstant.getNano() / 1000 == record.get("datetime")
    }

    def "should convert long time to timestamp-micros"() {
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

    def "should fail if date time micros is malformed"() {
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

    def "should fail if date micros is not a String nor a Long"() {
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

}
