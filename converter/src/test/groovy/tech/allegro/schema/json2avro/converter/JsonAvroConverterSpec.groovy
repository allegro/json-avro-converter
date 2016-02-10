package tech.allegro.schema.json2avro.converter

import groovy.json.JsonSlurper
import spock.lang.Specification

class JsonAvroConverterSpec extends Specification {

    def converter = new JsonAvroConverter()
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

    def toMap(String json) {
        slurper.parseText(json)
    }

    def toMap(byte[] json) {
        slurper.parse(json)
    }
}