package tech.allegro.schema.json2avro.converter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class IntDateConverterSpec extends BaseConverterSpec {

    def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "date",
                    "type" : {
                      "type" : "int",
                      "logicalType" : "date"
                    }
                  }
              ]
            }
        '''

    def "should convert iso date string to date"() {
        given:
        def json = '''
        {
            "date": "1970-01-02"
        }
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        1 == record.get("date")
    }

    def "should convert long epoch days to date"() {
        given:
        def json = '''
        {
            "date": 123
        }
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        123 == record.get("date")
    }

    def "should fail if date is malformed"() {
        given:
        def json = '''
        {
            "date": "test"
        }
        '''

        when:
        converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Field date should be a valid date."
    }

    def "should fail if date is not a String nor a Integer"() {
        given:
        def json = '''
        {
            "date": {}
        }
        '''

        when:
        converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Field date is expected to be type: java.lang.String or java.lang.Number."
    }

}
