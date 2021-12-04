package tech.allegro.schema.json2avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.Deque;
import java.util.Map;

public interface JsonToAvroReader {
    /**
     * convert a Map to a generic record
     *
     * @param json the json to convert
     * @param schema the avro schema to use
     *
     * @return the converted Record
     */
    GenericData.Record read(Map<String, Object> json, Schema schema);

    /**
     * allow to convert a json field to type corresponding avro type
     *
     * @param field the avro field to create
     * @param schema the schema associated to the field
     * @param jsonValue the json jsonValue of the field
     * @param path the path of the field on the json
     * @param silently should be false to throw an error in case of incompatible java type for the avro type
     *
     * @return the converted jsonValue
     */
    Object read(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently);
}
