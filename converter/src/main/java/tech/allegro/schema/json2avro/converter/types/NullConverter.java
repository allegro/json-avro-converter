package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.Schema;

import java.util.Deque;

public class NullConverter implements AvroTypeConverter {
    public static final NullConverter INSTANCE = new NullConverter();

    private NullConverter() {

    }

    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        return jsonValue == null ? null : new Incompatible("NULL");
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> path) {
        return schema.getType().equals(Schema.Type.NULL);
    }
}
