package tech.allegro.schema.json2avro.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AdditionalPropertyFieldTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TextNode TEXT = new TextNode("value");
  private static final BooleanNode BOOLEAN = BooleanNode.TRUE;
  private static final LongNode LONG = new LongNode(9001);
  private static final DoubleNode DOUBLE = new DoubleNode(9.001);
  private static final ArrayNode ARRAY = new ArrayNode(JsonNodeFactory.instance, List.of(TEXT, BOOLEAN, LONG, DOUBLE));
  private static final String ARRAY_STRING = "[\"value\",true,9001,9.001]";
  private static final ObjectNode OBJECT = MAPPER.createObjectNode();
  private static final String OBJECT_STRING = "{\"textNode\":\"value\"," +
      "\"booleanNode\":true," +
      "\"longNode\":9001," +
      "\"doubleNode\":9.001," +
      "\"arrayNode\":[\"value\",true,9001,9.001]}";

  static {
    OBJECT.set("textNode", TEXT);
    OBJECT.set("booleanNode", BOOLEAN);
    OBJECT.set("longNode", LONG);
    OBJECT.set("doubleNode", DOUBLE);
    OBJECT.set("arrayNode", ARRAY);
  }

  @Test
  public void testGetValue() {
    assertEquals("value", AdditionalPropertyField.getValue(TEXT));
    assertEquals("true", AdditionalPropertyField.getValue(BOOLEAN));
    assertEquals("9001", AdditionalPropertyField.getValue(LONG));
    assertEquals("9.001", AdditionalPropertyField.getValue(DOUBLE));
    assertEquals(ARRAY_STRING, AdditionalPropertyField.getValue(ARRAY));
    assertEquals(OBJECT_STRING, AdditionalPropertyField.getValue(OBJECT));
  }

  @Test
  public void testGetObjectValues() {
    final Map<String, Object> objectMap = Map.of(
        "textNode", TEXT,
        "booleanNode", BOOLEAN,
        "longNode", LONG,
        "doubleNode", DOUBLE,
        "arrayNode", ARRAY,
        "objectNode", OBJECT);

    final Map<String, String> objectStringMap = Map.of(
        "textNode", "value",
        "booleanNode", "true",
        "longNode", "9001",
        "doubleNode", "9.001",
        "arrayNode", ARRAY_STRING,
        "objectNode", OBJECT_STRING);
    assertEquals(objectStringMap, AdditionalPropertyField.getObjectValues(objectMap));
  }

  @Test
  public void testSerialize() throws JsonProcessingException {
    final TextNode textNode = new TextNode("value");
    assertEquals("value", AdditionalPropertyField.serialize(textNode));

    // this assertion shows that serialization with writeValueAsString
    // will result in extra quotation marks, which is not desirable
    assertEquals("\"value\"", MAPPER.writeValueAsString(textNode));
  }

}
