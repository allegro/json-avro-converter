package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.Schema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.function.Function;


public class PrimitiveConverter<T> extends AvroTypeConverterWithStrictJavaTypeCheck<T> {
    public final static AvroTypeConverter BOOLEAN = new PrimitiveConverter<>(Schema.Type.BOOLEAN, Boolean.class, bool -> bool);
    public final static AvroTypeConverter STRING = new PrimitiveConverter<>(Schema.Type.STRING, String.class, string -> string);
    public final static AvroTypeConverter INT = new PrimitiveConverter<>(Schema.Type.INT, Number.class, Number::intValue);
    public final static AvroTypeConverter LONG = new PrimitiveConverter<>(Schema.Type.LONG, Number.class, Number::longValue);
    public final static AvroTypeConverter DOUBLE = new PrimitiveConverter<>(Schema.Type.DOUBLE, Number.class, Number::doubleValue);
    public final static AvroTypeConverter FLOAT = new PrimitiveConverter<>(Schema.Type.FLOAT, Number.class, Number::floatValue);
    public final static AvroTypeConverter BYTES = new PrimitiveConverter<>(Schema.Type.BYTES, String.class, value -> ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));

    private final Schema.Type avroType;
    private final Function<T, Object> mapper;

    protected PrimitiveConverter(Schema.Type avroType, Class<T> javaType, Function<T, Object> mapper) {
        super(javaType);
        this.avroType = avroType;
        this.mapper = mapper;
    }

    @Override
    public Object convertValue(Schema.Field field, Schema schema, T value, Deque<String> path, boolean silently) {
        return mapper.apply(value);
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> path) {
        return schema.getType().equals(avroType);
    }

}
