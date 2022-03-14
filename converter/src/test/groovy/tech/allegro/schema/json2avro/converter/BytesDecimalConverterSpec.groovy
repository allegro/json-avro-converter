package tech.allegro.schema.json2avro.converter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import java.nio.ByteBuffer

class BytesDecimalConverterSpec extends BaseConverterSpec {

    def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "byteDecimal",
                    "type" : {
                      "type" : "bytes",
                      "logicalType" : "decimal",
                      "precision": 15, 
                      "scale": 5
                    }
                  }
              ]
            }
        '''

    def schemaWithNullableDecimal = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "byteDecimal",
                    "type" : [{
                      "type" : "bytes",
                      "logicalType" : "decimal",
                      "precision": 15, 
                      "scale": 5
                    }, "null"]
                  }
              ]
            }
        '''

    def "should convert json numeric to avro decimal"() {
        given:
        def json = '''
        {
            "byteDecimal": 123.456
        }
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        new BigDecimal("123.45600") == new BigDecimal(new BigInteger(((ByteBuffer) record.get("byteDecimal")).array()), 5)
    }

    def "should convert json string to avro decimal"() {
        given:
        def json = '''
        {
            "byteDecimal": "123.456"
        }
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        new BigDecimal("123.45600") == new BigDecimal(new BigInteger(((ByteBuffer) record.get("byteDecimal")).array()), 5)
    }

    def "should throw error when decimal json string is not a number"() {
        given:
        def json = '''
        {
            "byteDecimal": "test"
        }
        '''

        when:
        converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Field byteDecimal is expected to be a valid number. current value is test."
    }

    def "should throw error when decimal is malformed on a union type"() {
        given:
        def json = '''
        {
            "byteDecimal": "test"
        }
        '''

        when:
        converter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schemaWithNullableDecimal))

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Could not evaluate union, field byteDecimal is expected to be one of these: string number, decimal, NULL. If this is a complex type, check if offending field: byteDecimal adheres to schema."
    }

}
