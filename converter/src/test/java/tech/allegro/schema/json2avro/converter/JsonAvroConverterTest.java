package tech.allegro.schema.json2avro.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class JsonAvroConverterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();

  @SuppressWarnings("UnstableApiUsage")
  public static String readResource(final String name) throws IOException {
    final URL resource = Resources.getResource(name);
    return Resources.toString(resource, StandardCharsets.UTF_8);
  }

  private static <T> List<T> toList(final Iterator<T> iterator) {
    final List<T> list = new ArrayList<>();
    while (iterator.hasNext()) {
      list.add(iterator.next());
    }
    return list;
  }

  private static <T> Set<T> toSet(final Iterator<T> iterator) {
    final Set<T> set = new HashSet<>();
    while (iterator.hasNext()) {
      set.add(iterator.next());
    }
    return set;
  }

  public static class JsonToAvroConverterTestCaseProvider implements ArgumentsProvider {
    private static Function<String, String> getNameTransformer(final JsonNode testCase) {
      if (testCase.has("toUpperCase") && testCase.get("toUpperCase").asBoolean()) {
        return String::toUpperCase;
      } else {
        return Function.identity();
      }
    }

    private static Set<String> getJsonExtraPropsFields(final JsonNode testCase) {
      if (!testCase.has("jsonExtraPropsFields")) {
        return AdditionalPropertyField.DEFAULT_JSON_FIELD_NAMES;
      }
      return toSet(testCase.withArray("jsonExtraPropsFields").elements())
          .stream()
          .map(JsonNode::asText)
          .collect(Collectors.toSet());
    }

    private static String getAvroExtraPropsField(final JsonNode testCase) {
      if (!testCase.has("avroExtraPropsField")) {
        return AdditionalPropertyField.DEFAULT_AVRO_FIELD_NAME;
      }
      return testCase.get("avroExtraPropsField").asText();
    }

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
      final JsonNode testCases = JsonHelper.deserialize(readResource("json_avro_converter.json"));
      return toList(testCases.elements()).stream().map(testCase -> Arguments.of(
          testCase.get("testCase").asText(),
          testCase.get("avroSchema"),
          testCase.get("jsonObject"),
          testCase.get("avroObject"),
          getNameTransformer(testCase),
          getJsonExtraPropsFields(testCase),
          getAvroExtraPropsField(testCase)));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(JsonToAvroConverterTestCaseProvider.class)
  public void testJsonToAvroConverter(String testCaseName,
                                      JsonNode avroSchema,
                                      JsonNode jsonObject,
                                      JsonNode avroObject,
                                      Function<String, String> nameTransformer,
                                      Set<String> jsonExtraPropsFieldNames,
                                      String avroExtraPropsFieldName)
      throws JsonProcessingException {
    final JsonAvroConverter converter = JsonAvroConverter.builder()
        .setNameTransformer(nameTransformer)
        .setJsonAdditionalPropsFieldNames(jsonExtraPropsFieldNames)
        .setAvroAdditionalPropsFieldName(avroExtraPropsFieldName)
        .build();
    final Schema schema =  new Schema.Parser().parse(JsonHelper.serialize(avroSchema));
    final GenericData.Record actualAvroObject = converter.convertToGenericDataRecord(WRITER.writeValueAsBytes(jsonObject), schema);
    assertEquals(avroObject, JsonHelper.deserialize(actualAvroObject.toString()), String.format("Test for %s failed", testCaseName));
  }

}
