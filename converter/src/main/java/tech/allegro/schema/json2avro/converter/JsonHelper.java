package tech.allegro.schema.json2avro.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class JsonHelper {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static <T> String serialize(final T object) {
    try {
      return MAPPER.writeValueAsString(object);
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static JsonNode deserialize(final String jsonString) {
    try {
      return MAPPER.readTree(jsonString);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> JsonNode jsonNode(final T object) {
    return MAPPER.valueToTree(object);
  }

}
