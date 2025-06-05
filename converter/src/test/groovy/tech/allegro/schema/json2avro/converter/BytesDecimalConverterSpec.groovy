package tech.allegro.schema.json2avro.converter

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import tech.allegro.schema.json2avro.converter.types.BytesDecimalConverter

import java.math.RoundingMode
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
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        new BigDecimal("123.45600") == new BigDecimal(new BigInteger(((ByteBuffer) record.get("byteDecimal")).array()), 5)
    }

    def "should throw Exception for json numeric with higher scale than expected"() {
        given:
        def json = '''
        {
            "byteDecimal": 123.456789
        }
        '''

        when:
        avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Field byteDecimal is expected to be a number with scale up to 5. current value: 123.456789 is number with scale 6."
    }

    def "should be able to use rounding during conversion of numeric with higher scale than expected"() {
        given:
        def json = '''
        {
            "byteDecimal": 123.456789
        }
        '''
        def roundingConverter = new BytesDecimalConverter() {
            @Override
            protected BigDecimal bigDecimalWithExpectedScale(String decimal, int scale, Deque<String> path) {
                BigDecimal bigDecimalInput = new BigDecimal(decimal);
                return bigDecimalInput.setScale(scale, RoundingMode.DOWN);
            }
        }
        def converterWithRoundingDecimalConverter = new JsonAvroConverter(new ObjectMapper(),
                new CompositeJsonToAvroReader(Collections.singletonList(roundingConverter),
                        {name, value, path -> println "Unknown field $path with value $value"}))

        when:
        GenericData.Record record = converterWithRoundingDecimalConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        new BigDecimal("123.45678") == new BigDecimal(new BigInteger(((ByteBuffer) record.get("byteDecimal")).array()), 5)
    }

    def "should convert json string to avro decimal"() {
        given:
        def json = '''
        {
            "byteDecimal": "123.456"
        }
        '''

        when:
        GenericData.Record record = avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

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
        avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

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
        avroConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schemaWithNullableDecimal))

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Could not evaluate union, field byteDecimal is expected to be one of these: string number, decimal, NULL. If this is a complex type, check if offending field: byteDecimal adheres to schema."
    }

}
