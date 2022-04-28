package tech.allegro.schema.json2avro.converter

import org.apache.avro.AvroRuntimeException
import org.apache.avro.Schema
import org.apache.avro.data.RecordBuilderBase
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.GenericData

// copy-paste from GenericRecordBuilder with changed GenericData
class LogicalTypeGenericRecordBuilder extends RecordBuilderBase<GenericData.Record> {

    static LOGICAL_TYPE_GENERIC_DATA = new GenericData()

    static {
        LOGICAL_TYPE_GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.DateConversion())
    }

    private GenericData.Record record;

    LogicalTypeGenericRecordBuilder(Schema schema) {
        super(schema, LOGICAL_TYPE_GENERIC_DATA)
        record = new GenericData.Record(schema)
    }

    LogicalTypeGenericRecordBuilder set(Schema.Field field, Object value) {
        record.put(field.pos(), value);
        fieldSetFlags()[field.pos()] = true;
        return this;
    }

    @Override
    GenericData.Record build() {
        GenericData.Record record;
        try {
            record = new GenericData.Record(schema());
        } catch (Exception e) {
            throw new AvroRuntimeException(e)
        }

        for (Schema.Field field : fields()) {
            Object value;
            try {
                value = getWithDefault(field)
            } catch (IOException e) {
                throw new AvroRuntimeException(e);
            }
            if (value != null) {
                record.put(field.pos(), value)
            }
        }

        return record
    }

    private Object getWithDefault(Schema.Field field) throws IOException {
        return fieldSetFlags()[field.pos()] ? record.get(field.pos()) : defaultValue(field);
    }

}
