package tech.allegro.schema.json2avro.converter;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.*;

public class JsonGenericRecordReader {
    private ObjectMapper mapper;

    public JsonGenericRecordReader() {
        this.mapper = new ObjectMapper();
    }

    public JsonGenericRecordReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @SuppressWarnings("unchecked")
    public GenericData.Record read(byte[] data, Schema schema) {
        try {
            return read(mapper.readValue(data, Map.class), schema);
        } catch (IOException ex) {
            throw new AvroConversionException("Failed to parse json to map format.", ex);
        }
    }

    public GenericData.Record read(Map<String,Object> json, Schema schema) {
        Deque<String> path = new ArrayDeque<>();
        try {
            return readRecord(json, schema, path);
        } catch (AvroRuntimeException ex) {
            throw new AvroConversionException("Failed to convert JSON to Avro", ex);
        }
    }

    private GenericData.Record readRecord(Map<String,Object> json, Schema schema, Deque<String> path) {
            GenericRecordBuilder record = new GenericRecordBuilder(schema);
            json.entrySet().forEach(entry ->
                    ofNullable(schema.getField(entry.getKey()))
                            .ifPresent(field -> record.set(field, read(field, field.schema(), entry.getValue(), path))));
            return record.build();
    }

    @SuppressWarnings("unchecked")
    private Object read(Schema.Field field, Schema schema, Object value, Deque<String> path) {
        boolean pushed = !field.name().equals(path.peek());
        if(pushed) {
            path.push(field.name());
        }
        Object result;

        switch (schema.getType()) {
            case RECORD:  result = readRecord(ensureType(value, Map.class, path), schema, path); break;
            case ARRAY:   result =  readArray(field, schema, ensureType(value, List.class, path), path); break;
            case MAP:     result =  readMap(field, schema, ensureType(value, Map.class, path), path); break;
            case UNION:   result =  readUnion(field, schema, value, path); break;
            case INT:     result =  ensureType(value, Number.class, path).intValue(); break;
            case LONG:    result =  ensureType(value, Number.class, path).longValue(); break;
            case FLOAT:   result =  ensureType(value, Number.class, path).floatValue(); break;
            case DOUBLE:  result =  ensureType(value, Number.class, path).doubleValue(); break;
            case BOOLEAN: result =  ensureType(value, Boolean.class, path); break;
            case ENUM:    result =  ensureType(value, String.class, path); break;
            case STRING:  result =  ensureType(value, String.class, path); break;
            case NULL:    result =  ensureNull(value, path); break;
            default: throw new AvroTypeException("Unsupported type: " + field.schema().getType());
        }

        if(pushed) {
            path.pop();
        }
        return result;
    }

    private List<Object> readArray(Schema.Field field, Schema schema, List<Object> items, Deque<String> path) {
        return items.stream().map(item -> read(field, schema.getElementType(), item, path)).collect(toList());
    }

    private Map<String, Object> readMap(Schema.Field field, Schema schema, Map<String, Object> map, Deque<String> path) {
        return map.entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, entry -> read(field, schema.getValueType(), entry.getValue(), path)));
    }


    private Object readUnion(Schema.Field field, Schema schema, Object value, Deque<String> path) {
        List<Schema> types = schema.getTypes();
        for (Schema type : types) {
            try {
                return read(field, type, value, path);
            } catch (AvroRuntimeException ex) {
                continue;
            }
        }
        throw new AvroTypeException(format("Could not evaluate union, field %s is expected to be one of these: %s. " +
                "If this is a complex type, check if offending field: %s adheres to schema.",
                field.name(), types.stream().map(Schema::getType).map(Object::toString).collect(joining(",")), path(path)));
    }

    @SuppressWarnings("unchecked")
    private <T> T ensureType(Object value, Class<T> type, Deque<String> path) {
        if (type.isInstance(value)) {
            return (T) value;
        }
        throw new AvroTypeException(format("Field %s is expected to be of %s type.", path(path), type.getName()));
    }

    private Object ensureNull(Object o, Deque<String> path) {
        if (o != null) {
            throw new AvroTypeException(format("Field %s was expected to be null.", path(path)));
        }
        return null;
    }

    private String path(Deque<String> path) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(path.descendingIterator(), Spliterator.ORDERED), false)
            .map(Object::toString).collect(joining("."));
    }
}

