package tech.allegro.schema.json2avro.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import tech.allegro.schema.json2avro.converter.util.DateTimeUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static tech.allegro.schema.json2avro.converter.AdditionalPropertyField.DEFAULT_AVRO_FIELD_NAME;
import static tech.allegro.schema.json2avro.converter.AdditionalPropertyField.DEFAULT_JSON_FIELD_NAMES;
import static tech.allegro.schema.json2avro.converter.AvroTypeExceptions.enumException;
import static tech.allegro.schema.json2avro.converter.AvroTypeExceptions.typeException;
import static tech.allegro.schema.json2avro.converter.AvroTypeExceptions.unionException;

public class JsonGenericRecordReader {

    private static final Object INCOMPATIBLE = new Object();

    private final ObjectMapper mapper;
    private final UnknownFieldListener unknownFieldListener;
    private final Function<String, String> nameTransformer;
    // fields from the input json object that carry additional properties;
    // properties inside these fields will be added to the output extra props field
    private final Set<String> jsonExtraPropsFieldNames;
    // field in the output avro record that carries additional properties
    private final String avroExtraPropsFieldName;
    private final Field avroExtraPropsField;

    public static final class Builder {
        private ObjectMapper mapper = new ObjectMapper();
        private UnknownFieldListener unknownFieldListener;
        private Function<String, String> nameTransformer = Function.identity();
        private Set<String> jsonExtraPropsFields = DEFAULT_JSON_FIELD_NAMES;
        private String avroExtraPropsField = DEFAULT_AVRO_FIELD_NAME;

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

        public Builder setJsonAdditionalPropsFieldNames(Set<String> jsonAdditionalPropsFieldNames) {
            this.jsonExtraPropsFields = jsonAdditionalPropsFieldNames;
            return this;
        }

        public Builder setAvroAdditionalPropsFieldName(String avroAdditionalPropsFieldName) {
            this.avroExtraPropsField = avroAdditionalPropsFieldName;
            return this;
        }

        public JsonGenericRecordReader build() {
            return new JsonGenericRecordReader(mapper, unknownFieldListener, nameTransformer, jsonExtraPropsFields, avroExtraPropsField);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * @param nameTransformer          A function that transforms the field name.
     * @param jsonExtraPropsFieldNames A set of field names in the input Json object that are considered additional properties.
     *                                 All these fields will be stored in the additional properties field in the output Avro object.
     *                                 Default to ["_aibyte_additional_properties", "_ab_additional_properties"].
     * @param avroExtraPropsFieldName  Name of the field to store all the additional properties from the input Json object
     *                                 whose schema is not specified.
     *                                 Default to _aibyte_additional_properties.
     */
    private JsonGenericRecordReader(ObjectMapper mapper,
                                    UnknownFieldListener unknownFieldListener,
                                    Function<String, String> nameTransformer,
                                    Set<String> jsonExtraPropsFieldNames,
                                    String avroExtraPropsFieldName) {
        this.mapper = mapper;
        this.unknownFieldListener = unknownFieldListener;
        this.nameTransformer = nameTransformer;
        this.jsonExtraPropsFieldNames = jsonExtraPropsFieldNames;
        this.avroExtraPropsFieldName = avroExtraPropsFieldName;
        this.avroExtraPropsField = new Field(avroExtraPropsFieldName, AdditionalPropertyField.FIELD_SCHEMA, null, null);
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

    @SuppressWarnings("unchecked")
    private GenericData.Record readRecord(Map<String, Object> json, Schema schema, Deque<String> path) {
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        Map<String, String> additionalProps = new HashMap<>();
        boolean allowAdditionalProps = schema.getField(avroExtraPropsFieldName) != null;

        json.forEach((key, value) -> {
            if (value == null) {
                return;
            }

            String fieldName = nameTransformer.apply(key);
            Field field = schema.getField(fieldName);

            if (jsonExtraPropsFieldNames.contains(fieldName)) {
                additionalProps.putAll(AdditionalPropertyField.getObjectValues((Map<String, Object>) value));
            } else if (field != null) {
                record.set(fieldName, read(field, field.schema(), value, path, false));
            } else if (allowAdditionalProps) {
                additionalProps.put(fieldName, AdditionalPropertyField.getValue(value));
            } else if (unknownFieldListener != null) {
                unknownFieldListener.onUnknownField(key, value, PathsPrinter.print(path, key));
            }
        });

        if (allowAdditionalProps && additionalProps.size() > 0) {
            record.set(
                avroExtraPropsFieldName,
                read(avroExtraPropsField, AdditionalPropertyField.FIELD_SCHEMA, additionalProps, path, false));
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
        LogicalType logicalType = schema.getLogicalType();
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
                // Only "date" logical type is expected here, because the Avro schema is converted from a Json schema,
                // and this logical types corresponds to the Json "date" format.
                if (logicalType != null && logicalType.equals(LogicalTypes.date())) {
                    result = onValidType(value, String.class, path, silently, DateTimeUtils::getEpochDay);
                } else {
                    result = onValidNumber(value, path, silently, Number::intValue);
                }
                break;
            case LONG:
                // Only "time-micros" and "timestamp-micros" logical types are expected here, because
                // the Avro schema is converted from a Json schema, and the two logical types corresponds
                // to the Json "time" and "date-time" formats.
                if (logicalType != null && logicalType.equals(LogicalTypes.timestampMicros())) {
                    result = onValidType(value, String.class, path, silently, DateTimeUtils::getEpochMicros);
                } else if (logicalType != null && logicalType.equals(LogicalTypes.timeMicros())) {
                    result = onValidType(value, String.class, path, silently, DateTimeUtils::getMicroSeconds);
                } else {
                    result = onValidNumber(value, path, silently, Number::longValue);
                }
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
            Object result = function.apply((T) value);
            return result == null ? INCOMPATIBLE : result;
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

