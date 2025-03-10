package tech.allegro.schema.json2avro.converter


import org.apache.avro.Conversion
import org.apache.avro.LogicalType
import org.apache.avro.Schema

class AvroJsonConverterSpec extends BaseConverterSpec {

    def "should allow to customize the type conversion for a logical type"() {
        given:
            def additionalConversion = new Conversion<Integer>()  {

                @Override
                Class<Long> getConvertedType() {
                    return Integer.class
                }

                @Override
                String getLogicalTypeName() {
                    return "time-millis"
                }

                @Override
                Integer fromInt(Integer value, Schema schema, LogicalType type) {
                    return value
                }

                @Override
                Integer toInt(Integer value, Schema schema, LogicalType type) {
                    return value + 10
                }
            }

            def converter = new AvroJsonConverter(additionalConversion)
            def schema = '''
                {
                  "type" : "record",
                  "name" : "testSchema",
                  "fields" : [
                      {
                        "name" : "customString",
                        "type" : {
                          "type" : "int",
                          "logicalType" : "time-millis"
                        }
                      }
                  ]
                }
            '''

            def json = '''
            {
                "customString": 123
            }
            '''

            def avro = avroConverter.convertToAvro(json.bytes, new Schema.Parser().parse(schema))

        when:
            byte[] result = converter.convertToJson(avro, new Schema.Parser().parse(schema))

        then:
            toMap(result).customString == 133
    }
}
