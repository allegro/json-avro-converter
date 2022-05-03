package tech.allegro.schema.json2avro.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public final class AdditionalPropertyField {

  public static final String DEFAULT_AVRO_FIELD_NAME = "_airbyte_additional_properties";
  public static final Set<String> DEFAULT_JSON_FIELD_NAMES = ImmutableSet.of("_ab_additional_properties", DEFAULT_AVRO_FIELD_NAME);
  public static final Schema FIELD_SCHEMA = Schema.createUnion(
      Schema.create(Schema.Type.NULL),
      Schema.createMap(Schema.create(Type.STRING)));

  /**
   * Generate a map whose values are serialized strings.
   * E.g. given the following input:
   * {
   *   "a": 1,
   *   "b": {
   *     "c": 2
   *   }
   * }
   * the output will be:
   * "{ \"a\": 1, \"b\": { \"c\": 2 } }"
   *
   * The purpose of this method is to generate the serialized string value
   * for an object to store under the additional properties field.
   */
  public static Map<String, String> getObjectValues(Map<String, Object> json) {
    return json.entrySet().stream().collect(Collectors.toMap(
        Entry::getKey,
        r -> {
          JsonNode jsonNode = JsonHelper.jsonNode(r.getValue());
          return serialize(jsonNode);
        }));
  }

  /**
   * @return the serialized string value to store under the additional properties field.
   */
  public static String getValue(Object genericValue) {
    JsonNode jsonNode = JsonHelper.jsonNode(genericValue);
    return serialize(jsonNode);
  }

  @VisibleForTesting
  static String serialize(JsonNode jsonNode) {
    if (jsonNode.isTextual()) {
      // serialize a textual field this way to avoid additional quotation marks
      return jsonNode.asText();
    } else {
      return JsonHelper.serialize(jsonNode);
    }
  }

}
