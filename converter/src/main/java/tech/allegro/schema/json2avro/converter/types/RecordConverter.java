package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import tech.allegro.schema.json2avro.converter.PathsPrinter;
import tech.allegro.schema.json2avro.converter.JsonToAvroReader;
import tech.allegro.schema.json2avro.converter.UnknownFieldListener;

import java.util.Deque;
import java.util.Map;

public class RecordConverter extends AvroTypeConverterWithStrictJavaTypeCheck<Map> {
    private final JsonToAvroReader jsonToAvroReader;
    private final UnknownFieldListener unknownFieldListener;

    public RecordConverter(JsonToAvroReader jsonToAvroReader, UnknownFieldListener unknownFieldListener) {
        super(Map.class);
        this.jsonToAvroReader = jsonToAvroReader;
        this.unknownFieldListener = unknownFieldListener;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object convertValue(Schema.Field field, Schema schema, Map jsonValue, Deque<String> path, boolean silently) {
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        ((Map<String, Object>)jsonValue).forEach((key, value) -> {
            Schema.Field subField = schema.getField(key);
            if (subField != null) {
                record.set(subField, this.jsonToAvroReader.read(subField, subField.schema(), value, path, false));
            } else if (unknownFieldListener != null) {
                unknownFieldListener.onUnknownField(key, value, PathsPrinter.print(path, key));
            }
        });
        return record.build();
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> path) {
        return schema.getType().equals(Schema.Type.RECORD);
    }
}
