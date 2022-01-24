package tech.allegro.schema.json2avro.converter;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import tech.allegro.schema.json2avro.converter.types.ArrayConverter;
import tech.allegro.schema.json2avro.converter.types.EnumConverter;
import tech.allegro.schema.json2avro.converter.types.MapConverter;
import tech.allegro.schema.json2avro.converter.types.NullConverter;
import tech.allegro.schema.json2avro.converter.types.PrimitiveConverter;
import tech.allegro.schema.json2avro.converter.types.RecordConverter;
import tech.allegro.schema.json2avro.converter.types.AvroTypeConverter;
import tech.allegro.schema.json2avro.converter.types.UnionConverter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CompositeJsonToAvroReader implements JsonToAvroReader {
    private final List<AvroTypeConverter> converters;
    private final AvroTypeConverter mainRecordConverter;

    public CompositeJsonToAvroReader() {
        this(Collections.emptyList(), new FailOnUnknownField());
    }

    /**
     * this constructor allows you to specify custom converters. It can be used to convert custom java types, or avro schema with logical type.
     *
     * @param additionalConverters additional converters that implement {@link AvroTypeConverter}. These converters will override default converters.
     */
    public CompositeJsonToAvroReader(List<AvroTypeConverter> additionalConverters) {
        this(additionalConverters, new FailOnUnknownField());
    }

    /**
     * this constructor allows you to specify custom converters. It can be used to convert custom java types, or avro schema with logical type.
     *
     * @param additionalConverters additional converters that implement {@link AvroTypeConverter}. These converters will override default converters.
     */
    public CompositeJsonToAvroReader(AvroTypeConverter... additionalConverters) {
        this(Arrays.asList(additionalConverters));
    }

    /**
     * this constructor allows you to specify custom converters. It can be used to convert custom java types, or avro schema with logical type.
     *
     * @param additionalConverters additional converters that implement {@link AvroTypeConverter}. These converters will override default converters.
     * @param unknownFieldListener the listener to customize unknown field error management
     */
    public CompositeJsonToAvroReader(List<AvroTypeConverter> additionalConverters, UnknownFieldListener unknownFieldListener) {
        this.mainRecordConverter = new RecordConverter(this, unknownFieldListener);
        this.converters = new ArrayList<>();
        this.converters.addAll(additionalConverters);
        this.converters.add(PrimitiveConverter.BOOLEAN);
        this.converters.add(PrimitiveConverter.STRING);
        this.converters.add(PrimitiveConverter.INT);
        this.converters.add(PrimitiveConverter.LONG);
        this.converters.add(PrimitiveConverter.DOUBLE);
        this.converters.add(PrimitiveConverter.FLOAT);
        this.converters.add(PrimitiveConverter.BYTES);
        this.converters.add(EnumConverter.INSTANCE);
        this.converters.add(NullConverter.INSTANCE);
        this.converters.add(mainRecordConverter);
        this.converters.add(new ArrayConverter(this));
        this.converters.add(new MapConverter(this));
        this.converters.add(new UnionConverter(this));
    }

    @Override
    public GenericData.Record read(Map<String, Object> json, Schema schema) {
        return (GenericData.Record) this.mainRecordConverter.convert(null, schema, json, new ArrayDeque<>(), false);
    }

    @Override
    public Object read(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        boolean pushed = !field.name().equals(path.peekLast());
        if (pushed) {
            path.addLast(field.name());
        }

        AvroTypeConverter converter = this.converters.stream()
                .filter(c -> c.canManage(schema, path))
                .findFirst()
                .orElseThrow(() -> new AvroTypeException("Unsupported type: " + field.schema().getType()));
        Object result = converter.convert(field, schema, jsonValue, path, silently);

        if (pushed) {
            path.removeLast();
        }
        return result;
    }
}
