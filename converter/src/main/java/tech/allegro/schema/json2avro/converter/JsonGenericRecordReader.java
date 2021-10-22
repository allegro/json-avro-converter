package tech.allegro.schema.json2avro.converter;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static tech.allegro.schema.json2avro.converter.AvroTypeExceptions.enumException;
import static tech.allegro.schema.json2avro.converter.AvroTypeExceptions.typeException;
import static tech.allegro.schema.json2avro.converter.AvroTypeExceptions.unionException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;

public class JsonGenericRecordReader {
    private static final Object INCOMPATIBLE = new Object();
    private final ObjectMapper mapper;
    private final UnknownFieldListener unknownFieldListener;
    private final Function<String, String> nameTransformer;

    public static final class Builder {
        private ObjectMapper mapper = new ObjectMapper();
        private UnknownFieldListener unknownFieldListener;
        private Function<String, String> nameTransformer = Function.identity();

        private Builder() {
        }

        public Builder setObjectMapper(ObjectMapper mapper) {
            this.mapper = mapper;
            return this;
        }

        public Builder setUnknownFieldListener(UnknownFieldListener unknownFieldListener) {
            this.unknownFieldListener = unknownFieldListener;
            return this;
        }

        public Builder setNameTransformer(Function<String, String> nameTransformer) {
            this.nameTransformer = nameTransformer;
            return this;
        }

        public JsonGenericRecordReader build() {
            return new JsonGenericRecordReader(mapper, unknownFieldListener, nameTransformer);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private JsonGenericRecordReader(ObjectMapper mapper,
                                    UnknownFieldListener unknownFieldListener,
                                    Function<String, String> nameTransformer) {
        this.mapper = mapper;
        this.unknownFieldListener = unknownFieldListener;
        this.nameTransformer = nameTransformer;
    }

    @SuppressWarnings("unchecked")
    public GenericData.Record read(byte[] data, Schema schema) {
        try {
            return read(mapper.readValue(data, Map.class), schema);
        } catch (IOException ex) {
            throw new AvroConversionException("Failed to parse json to map format.", ex);
        }
    }

    public GenericData.Record read(Map<String, Object> json, Schema schema) {
        Deque<String> path = new ArrayDeque<>();
        try {
            return readRecord(json, schema, path);
        } catch (AvroTypeException ex) {
            throw new AvroConversionException("Failed to convert JSON to Avro: " + ex.getMessage(), ex);
        } catch (AvroRuntimeException ex) {
            throw new AvroConversionException("Failed to convert JSON to Avro", ex);
        }
    }

    private GenericData.Record readRecord(Map<String, Object> json, Schema schema, Deque<String> path) {
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        Map<String, String> additionalProperties = new HashMap<>();
        boolean allowAdditionalProperties = schema.getField(AdditionalPropertyField.FIELD_NAME) != null;
        json.entrySet().forEach(entry -> {
            String fieldName = nameTransformer.apply(entry.getKey());
            Field field = schema.getField(fieldName);
            if (field != null) {
                if (fieldName.equals(AdditionalPropertyField.FIELD_NAME)) {
                    additionalProperties.putAll(AdditionalPropertyField.getMapValue(entry.getValue()));
                } else {
                    record.set(fieldName, read(field, field.schema(), entry.getValue(), path, false));
                }
            } else if (allowAdditionalProperties) {
                additionalProperties.put(fieldName, AdditionalPropertyField.getValue(entry.getValue()));
            } else if (unknownFieldListener != null) {
                unknownFieldListener.onUnknownField(entry.getKey(), entry.getValue(), PathsPrinter.print(path, entry.getKey()));
            }
        });
        if (allowAdditionalProperties && additionalProperties.size() > 0) {
            record.set(AdditionalPropertyField.FIELD_NAME,
                read(AdditionalPropertyField.FIELD, AdditionalPropertyField.FIELD_SCHEMA, additionalProperties, path, false));
        }
        return record.build();
    }

    @SuppressWarnings("unchecked")
    private Object read(Schema.Field field, Schema schema, Object value, Deque<String> path, boolean silently) {
        String fieldName = nameTransformer.apply(field.name());
        boolean pushed = !fieldName.equals(path.peekLast());
        if (pushed) {
            path.addLast(fieldName);
        }
        Object result;

        switch (schema.getType()) {
            case RECORD:
                result = onValidType(value, Map.class, path, silently, map -> readRecord(map, schema, path));
                break;
            case ARRAY:
                result = onValidType(value, List.class, path, silently, list -> readArray(field, schema, list, path));
                break;
            case MAP:
                result = onValidType(value, Map.class, path, silently, map -> readMap(field, schema, map, path));
                break;
            case UNION:
                result = readUnion(field, schema, value, path);
                break;
            case INT:
                result = onValidNumber(value, path, silently, Number::intValue);
                break;
            case LONG:
                result = onValidNumber(value, path, silently, Number::longValue);
                break;
            case FLOAT:
                result = onValidNumber(value, path, silently, Number::floatValue);
                break;
            case DOUBLE:
                result = onValidNumber(value, path, silently, Number::doubleValue);
                break;
            case BOOLEAN:
                result = onValidType(value, Boolean.class, path, silently, bool -> bool);
                break;
            case ENUM:
                result = onValidType(value, String.class, path, silently, string -> ensureEnum(schema, string, path));
                break;
            case STRING:
                result = onValidType(value, String.class, path, silently, string -> string);
                break;
            case BYTES:
                result = onValidType(value, String.class, path, silently, string -> bytesForString(string));
                break;
            case NULL:
                result = value == null ? value : INCOMPATIBLE;
                break;
            default:
                throw new AvroTypeException("Unsupported type: " + field.schema().getType());
        }

        if (pushed) {
            path.removeLast();
        }
        return result;
    }

    private List<Object> readArray(Schema.Field field, Schema schema, List<Object> items, Deque<String> path) {
        return items.stream().map(item -> read(field, schema.getElementType(), item, path, false)).collect(toList());
    }

    private Map<String, Object> readMap(Schema.Field field, Schema schema, Map<String, Object> map, Deque<String> path) {
        Map<String, Object> result = new HashMap<>(map.size());
        map.forEach((k, v) -> result.put(k, read(field, schema.getValueType(), v, path, false)));
        return result;
    }

    private Object readUnion(Schema.Field field, Schema schema, Object value, Deque<String> path) {
        List<Schema> types = schema.getTypes();
        for (Schema type : types) {
            try {
                Object nestedValue = read(field, type, value, path, true);
                if (nestedValue == INCOMPATIBLE) {
                    continue;
                } else {
                    return nestedValue;
                }
            } catch (AvroRuntimeException e) {
                // thrown only for union of more complex types like records
                continue;
            }
        }
        throw unionException(
                field.name(),
                types.stream().map(Schema::getType).map(Object::toString).collect(joining(", ")),
                path);
    }

    private Object ensureEnum(Schema schema, Object value, Deque<String> path) {
        List<String> symbols = schema.getEnumSymbols();
        if (symbols.contains(value)) {
            return new GenericData.EnumSymbol(schema, value);
        }
        throw enumException(path, symbols.stream().map(String::valueOf).collect(joining(", ")));
    }

    private ByteBuffer bytesForString(String string) {
        return ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("unchecked")
    public <T> Object onValidType(Object value, Class<T> type, Deque<String> path, boolean silently, Function<T, Object> function)
            throws AvroTypeException {

        if (type.isInstance(value)) {
            return function.apply((T) value);
        } else {
            if (silently) {
                return INCOMPATIBLE;
            } else {
                throw typeException(path, type.getTypeName());
            }
        }
    }

    public Object onValidNumber(Object value, Deque<String> path, boolean silently, Function<Number, Object> function) {
        return onValidType(value, Number.class, path, silently, function);
    }
}

