package tech.allegro.schema.json2avro.converter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import java.time.LocalTime

class IntTimeMillisConverterSpec extends BaseConverterSpec {

    def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "time",
                    "type" : {
                      "type" : "int",
                      "logicalType" : "time-millis"
                    }
                  }
              ]
            }
        '''

    def "should convert iso time to time-millis"() {
        given:
        def json = '''
        {
            "time": "16:20:29.123Z"
        }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        def expectedTime = LocalTime.of(16, 20, 29, 123 * 1000_000)
        expectedTime.toNanoOfDay() / 1000_000 == record.get("time")
    }

    def "should convert int time to time-millis"() {
        given:
        def json = '''
        {
            "time": 123
        }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        123 == record.get("time")
    }

    def "should fail if time millis is malformed"() {
        given:
        def json = '''
        {
            "time": "test"
        }
        '''

        when:
        avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Field time should be a valid time."
    }

    def "should fail if time millis is not a String nor a Long"() {
        given:
        def json = '''
        {
            "time": {}
        }
        '''

        when:
        avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Field time is expected to be type: java.lang.String or java.lang.Number."
    }

}
