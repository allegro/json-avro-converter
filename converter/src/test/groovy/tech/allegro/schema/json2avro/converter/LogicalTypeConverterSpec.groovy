package tech.allegro.schema.json2avro.converter

import org.apache.avro.Schema
import org.apache.avro.data.RecordBuilderBase
import org.apache.avro.generic.GenericData
import spock.lang.Specification
import tech.allegro.schema.json2avro.converter.types.AvroTypeConverter
import tech.allegro.schema.json2avro.converter.types.IntDateConverter
import tech.allegro.schema.json2avro.converter.types.RecordConverter

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class LogicalTypeConverterSpec extends Specification {

    def logicalTypeIntDateConverter = new IntDateConverter(DateTimeFormatter.ISO_DATE) {
        @Override
        protected Object convertDateTimeString(String dateTimeString) {
            return parseLocalDate(dateTimeString)
        }
        @Override
        protected Object convertNumber(Number numberValue) {
            return LocalDate.ofEpochDay(super.convertNumber(numberValue) as int)
        }
    }

    def converter = new JsonAvroConverter(new CompositeJsonToAvroReader(Collections.singletonList(logicalTypeIntDateConverter)) {
        @Override
        protected AvroTypeConverter createMainConverter(UnknownFieldListener unknownFieldListener) {
            return new RecordConverter(this, unknownFieldListener) {
                @Override
                protected RecordBuilderBase<GenericData.Record> createRecordBuilder(Schema schema) {
                    new LogicalTypeGenericRecordBuilder(schema)
                }

                @Override
                protected void setField(RecordBuilderBase<GenericData.Record> builder, Schema.Field subField, Object fieldValue) {
                    (builder as LogicalTypeGenericRecordBuilder).set(subField, fieldValue)
                }
            }
        }
    })

    def schema = new Schema.Parser().parse('''
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "date",
                    "type" : {
                      "type" : "int",
                      "logicalType" : "date"
                    },
                    "default": 123
                  }
              ]
            }
        ''')

    def "should convert date string to LocalDate"() {
        given:
        def json = '''
        {
            "date": "1970-01-02"
        }
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, schema)

        then:
        LocalDate.ofEpochDay(1) == record.get("date")
    }

    def "should convert number to LocalDate"() {
        given:
        def json = '''
        {
            "date": 1.0
        }
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, schema)

        then:
        LocalDate.ofEpochDay(1) == record.get("date")
    }

    def "should return default value as a LocalDate"() {
        given:
        def json = '''
        {}
        '''

        when:
        GenericData.Record record = converter.convertToGenericDataRecord(json.bytes, schema)

        then:
        LocalDate.ofEpochDay(123) == record.get("date")
    }


}
