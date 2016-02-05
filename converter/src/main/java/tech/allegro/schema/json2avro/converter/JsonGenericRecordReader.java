package tech.allegro.schema.json2avro.converter;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

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
        try {
            GenericRecordBuilder record = new GenericRecordBuilder(schema);
            json.entrySet().forEach(entry ->
                    ofNullable(schema.getField(entry.getKey()))
                            .ifPresent(field -> record.set(field, read(field, field.schema(), entry.getValue()))));
            return record.build();
        } catch (AvroRuntimeException ex) {
            throw new AvroConversionException("Failed to convert json to avro.", ex);
        }
    }

    @SuppressWarnings("unchecked")
    private Object read(Schema.Field field, Schema schema, Object value) {
        switch (schema.getType()) {
            case RECORD:  return read(ensureType(value, Map.class, field), schema);
            case ARRAY:   return readArray(field, schema, ensureType(value, List.class, field));
            case MAP:     return readMap(field, schema, ensureType(value, Map.class, field));
            case UNION:   return readUnion(field, schema, value);
            case INT:     return ensureType(value, Number.class, field).intValue();
            case LONG:    return ensureType(value, Number.class, field).longValue();
            case FLOAT:   return ensureType(value, Number.class, field).floatValue();
            case DOUBLE:  return ensureType(value, Number.class, field).doubleValue();
            case BOOLEAN: return ensureType(value, Boolean.class, field);
            case ENUM:
            case STRING:  return ensureType(value, String.class, field);
            case NULL:    return ensureNull(value, field);
            default: throw new AvroTypeException("Unsupported type: " + field.schema().getType());
        }
    }

    private List<Object> readArray(Schema.Field field, Schema schema, List<Object> items) {
        return items.stream().map(item -> read(field, schema.getElementType(), item)).collect(toList());
    }

    private Map<String, Object> readMap(Schema.Field field, Schema schema, Map<String, Object> map) {
        return map.entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, entry -> read(field, schema.getValueType(), entry.getValue())));
    }


    private Object readUnion(Schema.Field field, Schema schema, Object value) {
        List<Schema> types = schema.getTypes();
        for (Schema type : types) {
            try {
                return read(field, type, value);
            } catch (AvroRuntimeException ex) {
                continue;
            }
        }
        throw new AvroTypeException(format("Could not evaluate union, field %s is expected to be one of these: %s.",
                field.name(), types.stream().map(Schema::getType).map(Object::toString).collect(joining(","))));
    }

    @SuppressWarnings("unchecked")
    private <T> T ensureType(Object value, Class<T> type, Schema.Field field) {
        if (type.isInstance(value)) {
            return (T) value;
        }
        throw new AvroTypeException(format("Field %s is expected to be of %s type.", field.name(), type.getName()));
    }

    private Object ensureNull(Object o, Schema.Field field) {
        if (o != null) {
            throw new AvroTypeException(format("Field %s was expected to be null.", field.name()));
        }
        return null;
    }
}

