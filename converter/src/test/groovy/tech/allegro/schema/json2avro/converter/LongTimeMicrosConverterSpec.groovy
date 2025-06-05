package tech.allegro.schema.json2avro.converter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import java.time.LocalTime

class LongTimeMicrosConverterSpec extends BaseConverterSpec {

    def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "time",
                    "type" : {
                      "type" : "long",
                      "logicalType" : "time-micros"
                    }
                  }
              ]
            }
        '''

    def "should convert iso time to time-micros"() {
        given:
        def json = '''
        {
            "time": "16:20:29.123456Z"
        }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        def expectedTime = LocalTime.of(16, 20, 29, 123456 * 1000)
        expectedTime.toNanoOfDay() / 1000 == record.get("time")
    }

    def "should convert long time to time-micros"() {
        given:
        def json = '''
        {
            "time": 1644078029000
        }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        1644078029000 == record.get("time")
    }

    def "should fail if time micros is malformed"() {
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

    def "should fail if time micros is not a String nor a Long"() {
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
