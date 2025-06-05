package tech.allegro.schema.json2avro.converter


import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import com.fasterxml.jackson.databind.ObjectMapper
import spock.lang.Unroll
import tech.allegro.schema.json2avro.converter.types.AvroTypeConverter

import java.time.LocalDateTime
import java.time.ZoneOffset;


class JsonAvroConverterSpec extends BaseConverterSpec {

    def "should convert record with primitives"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_integer",
                    "type" : "int"
                  },
                  {
                    "name" : "field_long",
                    "type" : "long"
                  },
                  {
                    "name" : "field_float",
                    "type" : "float"
                  },
                  {
                    "name" : "field_double",
                    "type" : "double"
                  },
                  {
                    "name" : "field_boolean",
                    "type" : "boolean"
                  },
                  {
                    "name" : "field_string",
                    "type" : "string"
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_integer": 1,
            "field_long": 2,
            "field_float": 1.1,
            "field_double": 1.2,
            "field_boolean": true,
            "field_string": "foobar"
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(jsonConverter.convertToJson(avro, schema))
    }

    def "should convert bytes generic record"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_bytes",
                    "type" : "bytes"
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_bytes": "\\u0001\\u0002\\u0003"
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(jsonConverter.convertToJson(avro, schema))
    }

    def "should throw exception when parsing record with mismatched primitives"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_integer",
                    "type" : "int"
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_integer": "foobar"
        }
        '''

        when:
        avroConverter.convertToAvro(json.bytes, schema)

        then:
        def e = thrown AvroConversionException
        e.message == "Failed to convert JSON to Avro: Field field_integer is expected to be type: java.lang.Number"
    }

    @Unroll
    def "should ignore unknown fields"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_string",
                    "type" : "string"
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_integer": 1,
            "field_long": 2,
            "field_float": 1.1,
            "field_double": 1.2,
            "field_boolean": true,
            "field_string": "foobar"
        }
        '''

        when:
        byte[] avro = converterNotFailingOnUnknown.convertToAvro(json.bytes, schema)

        then:
        def result = toMap(jsonConverter.convertToJson(avro, schema))
        result.field_string == "foobar"
        result.keySet().size() == 1

        where:
        converterNotFailingOnUnknown << [new JsonAvroConverter(), new JsonAvroConverter(new ObjectMapper())]
    }
    
    def "should fail unknown fields"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_string",
                    "type" : "string"
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_integer": 1,
            "field_long": 2,
            "field_float": 1.1,
            "field_double": 1.2,
            "field_boolean": true,
            "field_string": "foobar"
        }
        '''

        when:
        converterFailOnUnknown.convertToAvro(json.bytes, schema)

        then:
        thrown AvroConversionException
    }

    def "should throw exception when field is missing"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_integer",
                    "type" : "int"
                  },
                  {
                    "name" : "field_long",
                    "type" : "long"
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_integer": 1
        }
        '''

        when:
        avroConverter.convertToAvro(json.bytes, schema)

        then:
        thrown AvroConversionException
    }

    def "should convert message with nested records"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_record",
                    "type" : {
                        "name" : "string_type",
                        "type": "record",
                        "fields": [
                            {
                                "type": "string",
                                "name": "field_string"
                            }
                        ]
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_record": {
                "field_string": "foobar"
            }
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(jsonConverter.convertToJson(avro, schema))
    }

    def "should convert nested record with missing field"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_record",
                    "type" : {
                        "name" : "string_type",
                        "type": "record",
                        "fields": [
                            {
                                "type": "string",
                                "name": "field_string"
                            }
                        ]
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_foobar": 1
        }
        '''

        when:
        avroConverter.convertToAvro(json.bytes, schema)

        then:
        thrown AvroConversionException
    }

    def "should convert nested map of primitives"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_map",
                    "type" : {
                        "name" : "map_type",
                        "type": "map",
                        "values": "string"
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_map": {
                "foo": "bar"
            }
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(jsonConverter.convertToJson(avro, schema))
    }

    def "should fail when converting nested map with mismatched value type"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_map",
                    "type" : {
                        "name" : "map_type",
                        "type": "map",
                        "values": "string"
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_map": 1
        }
        '''

        when:
        avroConverter.convertToAvro(json.bytes, schema)

        then:
        thrown AvroConversionException
    }

    def "should convert nested map of records"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_map",
                    "type" : {
                        "name" : "map_type",
                        "type": "map",
                        "values": {
                            "name" : "string_type",
                            "type": "record",
                            "fields": [
                                {
                                    "type": "string",
                                    "name": "field_string"
                                }
                            ]
                        }
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_map": {
                "foo": {
                    "field_string": "foobar"
                }
            }
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(jsonConverter.convertToJson(avro, schema))
    }

    def "should convert nested array of primitives"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_array",
                    "type" : {
                        "name" : "array_type",
                        "type": "array",
                        "items": {
                            "name": "item",
                            "type": "string"
                        }
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_array": ["foo", "bar"]
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(jsonConverter.convertToJson(avro, schema))
    }

    def "should convert nested array of records"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_array",
                    "type" : {
                        "name" : "array_type",
                        "type": "array",
                        "items": {
                            "name" : "string_type",
                            "type": "record",
                            "fields": [
                                {
                                    "type": "string",
                                    "name": "field_string"
                                }
                            ]
                        }
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_array": [
                {
                    "field_string": "foo"
                },
                {
                    "field_string": "bar"
                }
            ]
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(jsonConverter.convertToJson(avro, schema))
    }

    def "should convert nested union of primitives"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_union",
                    "type" : ["string", "int"]
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_union": 8
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(jsonConverter.convertToJson(avro, schema))
    }

    def "should convert nested union of records"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_union",
                    "type" : [
                        {
                            "name" : "int_type",
                            "type": "record",
                            "fields": [
                                {
                                    "type": "int",
                                    "name": "field_int"
                                }
                            ]
                        },
                        {
                            "name" : "string_type",
                            "type": "record",
                            "fields": [
                                {
                                    "type": "string",
                                    "name": "field_string"
                                }
                            ]
                        }
                    ]
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_union": {
                "field_string": "foobar"
            }
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(jsonConverter.convertToJson(avro, schema))
    }

    def "should convert nested union with null and primitive should result in an optional field"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_string",
                    "type" : "string"
                  },
                  {
                    "name" : "field_union",
                    "type" : ["null", "int"],
                    "default": null
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_string": "foobar"
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        def result = toMap(jsonConverter.convertToJson(avro, schema))
        result.field_string == toMap(json).field_string
        result.field_union == null
    }

    def "should convert nested union with null and record should result in an optional field"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_string",
                    "type" : "string"
                  },
                  {
                    "name" : "field_union",
                    "type" : [
                        "null",
                        {
                            "name" : "int_type",
                            "type": "record",
                            "fields": [
                                {
                                    "type": "int",
                                    "name": "field_int"
                                }
                            ]
                        }
                    ],
                    "default": null
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_string": "foobar"
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        def result = toMap(jsonConverter.convertToJson(avro, schema))
        result.field_string == toMap(json).field_string
        result.field_union == null
    }

    def "should convert nested union with null and map should result in an optional field"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_string",
                    "type" : "string"
                  },
                  {
                    "name" : "field_union",
                    "type" : [
                        "null",
                        {
                            "name" : "map_type",
                            "type": "map",
                            "values": {
                                "name" : "string_type",
                                "type": "record",
                                "fields": [
                                    {
                                        "type": "string",
                                        "name": "field_string"
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_string": "foobar"
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        def result = toMap(jsonConverter.convertToJson(avro, schema))
        result.field_string == toMap(json).field_string
        result.field_union == null
    }

    def "should convert optional fields should not be wrapped when converting from avro to json"() {
        given:
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_string",
                    "type" : "string"
                  },
                  {
                    "name" : "field_union",
                    "type" : ["null", "int"],
                    "default": null
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_string": "foobar",
            "field_union": 1
        }
        '''

        when:
        def result = jsonConverter.convertToJson(avroConverter.convertToAvro(json.bytes, schema), schema)

        then:
        toMap(result) == toMap(json)
    }

    def "should print full path to invalid field on error"() {
        given:
        def schema = '''
            {
              "name": "testSchema",
              "type": "record",
              "fields": [
                {
                  "name": "field",
                  "default": null,
                  "type": [
                    "null",
                    {
                      "name": "field_type",
                      "type": "record",
                      "fields": [
                        {
                          "name": "stringValue",
                          "type": "string"
                        }
                      ]
                    }
                  ]
                }
              ]
            }
        '''

        def json = '''
        {
            "field": { "stringValue": 1 }
        }
        '''

        when:
        avroConverter.convertToJson(avroConverter.convertToAvro(json.bytes, schema), schema)

        then:
        def exception = thrown(AvroConversionException)
        exception.cause.message  ==~ /.*field\.stringValue.*/
    }

    def "should parse enum types properly"() {
        given:
        def schema = '''
            {
              "name": "testSchema",
              "type": "record",
              "fields": [
                  {
                    "name" : "field_enum",
                    "type" : {
                        "name" : "MyEnums",
                        "type" : "enum",
                        "symbols" : [ "A", "B", "C" ]
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_enum": "A"
        }
        '''

        when:
        def result = jsonConverter.convertToJson(avroConverter.convertToAvro(json.bytes, schema), schema)

        then:
        toMap(result) == toMap(json)
    }

    def "should throw the appropriate error when passing an invalid enum type"() {
        given:
        def schema = '''
            {
              "name": "testSchema",
              "type": "record",
              "fields": [
                  {
                    "name" : "field_enum",
                    "type" : {
                        "name" : "MyEnums",
                        "type" : "enum",
                        "symbols" : [ "A", "B", "C" ]
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "field_enum": "D"
        }
        '''

        when:
        avroConverter.convertToJson(avroConverter.convertToAvro(json.bytes, schema), schema)

        then:
        def exception = thrown(AvroConversionException)
        exception.cause.message  ==~ /.*enum type and be one of A, B, C.*/
    }

    def "should accept null when value can be of any nullable array/map type"() {
        given:
        def schema = '''
            {
                "type": "record",
                "name": "testSchema",
                "fields": [
                    {
                        "name": "payload",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items": "string"
                            },
                            {
                                "type": "map",
                                "values": [
                                    "null",
                                    "string",
                                    "int"
                                ]
                             }
                        ]
                    }
                ]
            }
        '''

        def json = '''
        {
            "payload": {
                "foo": null
            }
        }
        '''

        when:
        byte[] avro = avroConverter.convertToAvro(json.bytes, schema)

        then:
        !toMap(jsonConverter.convertToJson(avro, schema)).payload.foo
    }

    def "should convert specific record"() {
        given:
        def json = '''
        {
            "test": "test",
            "enumTest": "s1"
        }
        '''
        def clazz = SpecificRecordConvertTest.class
        def schema = SpecificRecordConvertTest.getClassSchema()

        when:
        SpecificRecordConvertTest result = avroConverter.convertToSpecificRecord(json.bytes, clazz, schema)
        then:
        result != null && result instanceof SpecificRecordConvertTest && result.getTest() == "test"
    }

    def "should convert specific record and back to json"() {
        given:
        def json = '''{"test":"test","enumTest":"s1"}'''
        def clazz = SpecificRecordConvertTest.class
        def schema = SpecificRecordConvertTest.getClassSchema()

        when:
        SpecificRecordConvertTest record = avroConverter.convertToSpecificRecord(json.bytes, clazz, schema)
        def result = jsonConverter.convertToJson(record)
        then:
        result != null && new String(result) == json
    }

    def "should allow to customize the avro type conversion for a logical-type"() {
        given:
        def now = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        def additionalConverter = new AvroTypeConverter() {
            @Override
            Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
                return now;
            }

            @Override
            boolean canManage(Schema schema, Deque<String> path) {
                return schema.getType() == Schema.Type.LONG && schema.getLogicalType().name == "timestamp-millis"
            }
        }

        def converterWithCustomConverter = new JsonAvroConverter(new ObjectMapper(), new CompositeJsonToAvroReader([additionalConverter]))
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "customDate",
                    "type" : {
                      "type" : "long",
                      "logicalType" : "timestamp-millis"
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "customDate": "now"
        }
        '''

        when:
        GenericData.Record record = converterWithCustomConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        now == record.get("customDate")
    }

    def "should allow to customize the avro type conversion for a field"() {
        given:
        def additionalConverter = new AvroTypeConverter() {
            @Override
            Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
                return "custom-" + jsonValue;
            }

            @Override
            boolean canManage(Schema schema, Deque<String> path) {
                return path.getLast() == "customString"
            }
        }

        def converterWithCustomConverter = new JsonAvroConverter(new CompositeJsonToAvroReader(additionalConverter))
        def schema = '''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "customString",
                    "type" : {
                      "type" : "long",
                      "logicalType" : "timestamp-millis"
                    }
                  }
              ]
            }
        '''

        def json = '''
        {
            "customString": "foo"
        }
        '''

        when:
        GenericData.Record record = converterWithCustomConverter.convertToGenericDataRecord(json.bytes, new Schema.Parser().parse(schema))

        then:
        "custom-foo" == record.get("customString")
    }
}
