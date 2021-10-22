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
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
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

  public static <T> List<T> toList(final Iterator<T> iterator) {
    final List<T> list = new ArrayList<>();
    while (iterator.hasNext()) {
      list.add(iterator.next());
    }
    return list;
  }

  public static class JsonToAvroConverterTestCaseProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
      final JsonNode testCases = JsonHelper.deserialize(readResource("json_avro_converter.json"));
      return toList(testCases.elements()).stream().map(testCase -> Arguments.of(
          testCase.get("testCase").asText(),
          testCase.get("avroSchema"),
          testCase.get("jsonObject"),
          testCase.get("avroObject"),
          testCase.has("name_transformer") && testCase.get("name_transformer").asBoolean()));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(JsonToAvroConverterTestCaseProvider.class)
  public void testJsonToAvroConverter(String testCaseName, JsonNode avroSchema, JsonNode jsonObject, JsonNode avroObject, boolean useNameTransformer)
      throws JsonProcessingException {
    final JsonAvroConverter converter = JsonAvroConverter.builder()
        .setNameTransformer(useNameTransformer ? String::toUpperCase : Function.identity())
        .build();
    final Schema schema =  new Schema.Parser().parse(JsonHelper.serialize(avroSchema));
    final GenericData.Record actualAvroObject = converter.convertToGenericDataRecord(WRITER.writeValueAsBytes(jsonObject), schema);
    assertEquals(avroObject, JsonHelper.deserialize(actualAvroObject.toString()), String.format("Test for %s failed", testCaseName));
  }

}
