package tech.allegro.schema.json2avro.converter;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

public class AdditionalPropertyField {

  public static final String FIELD_NAME = "_ab_additional_properties";
  public static final Schema FIELD_SCHEMA = Schema.createUnion(
      Schema.create(Schema.Type.NULL),
      Schema.createMap(Schema.create(Type.STRING)));
  public static final Field FIELD = new Field(FIELD_NAME, FIELD_SCHEMA, null, null);

  @SuppressWarnings("unchecked")
  public static Map<String, String> getMapValue(Object genericValue) {
    Map<String, Object> mapValue = (Map<String, Object>) genericValue;
    return mapValue.entrySet().stream().collect(Collectors.toMap(
        Entry::getKey,
        r -> {
          JsonNode jsonNode = JsonHelper.jsonNode(r.getValue());
          return serialize(jsonNode);
        }));
  }

  public static String getValue(Object genericValue) {
    JsonNode jsonNode = JsonHelper.jsonNode(genericValue);
    return serialize(jsonNode);
  }

  public static String serialize(JsonNode jsonNode) {
    if (jsonNode.isTextual()) {
      // serialize a textual field this way to avoid additional quotation marks
      return jsonNode.asText();
    } else {
      return JsonHelper.serialize(jsonNode);
    }
  }

}
