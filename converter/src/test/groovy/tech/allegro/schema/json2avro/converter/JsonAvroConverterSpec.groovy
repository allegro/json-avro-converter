package tech.allegro.schema.json2avro.converter

import java.nio.ByteBuffer
import java.util.function.Function

import static java.util.Collections.singletonMap;
import groovy.json.JsonSlurper
import spock.lang.Specification

import com.fasterxml.jackson.databind.ObjectMapper;

class JsonAvroConverterSpec extends Specification {
    class UUIDConverter implements Function<String, UUID> {
        @Override
        UUID apply(String input) {
            final java.util.UUID uuid = java.util.UUID.fromString(input)
            final ByteBuffer buffer = ByteBuffer.allocate(16)
            buffer.putLong(uuid.getMostSignificantBits())
            buffer.putLong(uuid.getLeastSignificantBits())
            new UUID(buffer.array())
        }
    }
    def converter = new JsonAvroConverter(new ObjectMapper(),
        {name, value, path -> println "Unknown field $path with value $value"})
    def converterFailOnUnknown = new JsonAvroConverter(new ObjectMapper(), new FailOnUnknownField())
    def converterCustomStringFieldMapping = new JsonAvroConverter(
          new ObjectMapper(),
          null,
          singletonMap("field_uuid_string", new UUIDConverter()
        )
    )
    def slurper = new JsonSlurper()

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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(converter.convertToJson(avro, schema))
    }


    def 'should convert bytes generic record'() {
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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(converter.convertToJson(avro, schema))
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
        converter.convertToAvro(json.bytes, schema)

        then:
        thrown AvroConversionException
    }

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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        def result = toMap(converter.convertToJson(avro, schema))
        result.field_string == "foobar"
        result.keySet().size() == 1
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
        converter.convertToAvro(json.bytes, schema)

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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(converter.convertToJson(avro, schema))
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
        converter.convertToAvro(json.bytes, schema)

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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(converter.convertToJson(avro, schema))
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
        converter.convertToAvro(json.bytes, schema)

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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(converter.convertToJson(avro, schema))
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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(converter.convertToJson(avro, schema))
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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(converter.convertToJson(avro, schema))
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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(converter.convertToJson(avro, schema))
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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        toMap(json) == toMap(converter.convertToJson(avro, schema))
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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        def result = toMap(converter.convertToJson(avro, schema))
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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        def result = toMap(converter.convertToJson(avro, schema))
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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        def result = toMap(converter.convertToJson(avro, schema))
        result.field_string == toMap(json).field_string
        result.field_union == null
    }

    def 'should convert optional fields should not be wrapped when converting from avro to json'() {
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
        def result = converter.convertToJson(converter.convertToAvro(json.bytes, schema), schema)

        then:
        toMap(result) == toMap(json)
    }

    def 'should print full path to invalid field on error'() {
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
        converter.convertToJson(converter.convertToAvro(json.bytes, schema), schema)

        then:
        def exception = thrown(AvroConversionException)
        exception.cause.message  ==~ /.*field\.stringValue.*/
    }

    def 'should parse enum types properly'() {
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
        def result = converter.convertToJson(converter.convertToAvro(json.bytes, schema), schema)

        then:
        toMap(result) == toMap(json)
    }

    def 'should throw the appropriate error when passing an invalid enum type'() {
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
        converter.convertToJson(converter.convertToAvro(json.bytes, schema), schema)

        then:
        def exception = thrown(AvroConversionException)
        exception.cause.message  ==~ /.*enum type and be one of A, B, C.*/
    }

    def 'should accept null when value can be of any nullable array/map type'() {
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
        byte[] avro = converter.convertToAvro(json.bytes, schema)

        then:
        !toMap(converter.convertToJson(avro, schema)).payload.foo
    }

    def 'should convert specific record'() {
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
        SpecificRecordConvertTest result = converter.convertToSpecificRecord(json.bytes, clazz, schema)
        then:
        result != null && result instanceof SpecificRecordConvertTest && result.getTest() == "test"
    }

    def 'should convert specific record and back to json'() {
        given:
        def json = '''{"test":"test","enumTest":"s1"}'''
        def clazz = SpecificRecordConvertTest.class
        def schema = SpecificRecordConvertTest.getClassSchema()

        when:
        SpecificRecordConvertTest record = converter.convertToSpecificRecord(json.bytes, clazz, schema)
        def result = converter.convertToJson(record)
        then:
        result != null && new String(result) == json
    }

    def 'should parse mapped UUID string field properly'() {
        given:
        def clazz = SpecificRecordUUIDConvertTest.class
        def schema = SpecificRecordUUIDConvertTest.getClassSchema()

        def uuidString = "5c7eb813-08d3-4bc5-90e3-e2b2893c77bb"

        def json = """
        {
            "field_uuid_string": "${uuidString}"
        }
        """

        when:
        SpecificRecordUUIDConvertTest record = converterCustomStringFieldMapping.convertToSpecificRecord(json.bytes, clazz, schema)

        then:
        record != null &&
        record.fieldUuidString.equals(
                new UUIDConverter().apply(uuidString)
        )
    }

    def toMap(String json) {
        slurper.parseText(json)
    }

    def toMap(byte[] json) {
        slurper.parse(json)
    }
}
