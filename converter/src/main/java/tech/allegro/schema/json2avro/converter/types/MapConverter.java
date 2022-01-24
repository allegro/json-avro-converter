package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.Schema;
import tech.allegro.schema.json2avro.converter.JsonToAvroReader;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class MapConverter extends AvroTypeConverterWithStrictJavaTypeCheck<Map> {
    private final JsonToAvroReader recordRecord;

    public MapConverter(JsonToAvroReader recordRecord) {
        super(Map.class);
        this.recordRecord = recordRecord;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object convertValue(Schema.Field field, Schema schema, Map jsonValue, Deque<String> path, boolean silently) {
        Map<String, Object> result = new HashMap<>(jsonValue.size());
        ((Map<String, Object>)jsonValue).forEach((k, v) ->
                result.put(k, this.recordRecord.read(field, schema.getValueType(), v, path, false))
        );
        return result;
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> path) {
        return schema.getType().equals(Schema.Type.MAP);
    }

}
