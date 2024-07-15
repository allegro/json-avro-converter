package tech.allegro.schema.json2avro.converter;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.allegro.schema.json2avro.converter.TestUtils.readResource;
import static tech.allegro.schema.json2avro.converter.TestUtils.toList;

public class FieldConversionFailureListenerTest {
    public static class FieldConversionFailureListenerTestProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
            final JsonNode desc = JsonHelper.deserialize(readResource("field_conversion_failure_listener.json"));
            return toList(desc.get("testCases").elements()).stream().map(testCase -> Arguments.of(
                    testCase.get("name").asText(),
                    desc.get("avroSchema"),
                    testCase.get("records"),
                    testCase.get("expectedOutput")));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(FieldConversionFailureListenerTestProvider.class)
    public void testFieldConversionFailureListener(String testCaseName,
                                                   JsonNode avroSchemaJson,
                                                   JsonNode recordsJson,
                                                   JsonNode expectedOutputJson){
        final Schema avroSchema = new Schema.Parser().parse(JsonHelper.serialize(avroSchemaJson));
        final Schema metaSchema = avroSchema.getField("META").schema();
        final Schema changesSchema = metaSchema.getField("CHANGES").schema().getElementType().getTypes().get(0);
        final List<String> expectedRecords = toList(expectedOutputJson.elements()).stream().map(JsonNode::toString).toList();

        final JsonAvroConverter converter = JsonAvroConverter.builder()
                .setNameTransformer(String::toUpperCase)
                .setFieldConversionFailureListener(new FieldConversionFailureListener() {
                    @Override
                    public Object onFieldConversionFailure(String avroName,
                                                           String originalName,
                                                           Schema schema,
                                                           Object value,
                                                           String path,
                                                           Exception exception) {
                        pushPostProcessingAction(record -> {
                            GenericData.Record change = new GenericRecordBuilder(changesSchema)
                                    .set("FIELD", originalName)
                                    .set("CHANGE", "NULLED")
                                    .set("REASON", exception.getMessage())
                                    .build();
                            GenericData.Record meta = (GenericData.Record) record.get("META");
                            Object changesObj = meta.get("CHANGES");
                            @SuppressWarnings("unchecked")
                            List<GenericData.Record> changes = (List<GenericData.Record>) changesObj;
                            changes.add(change);

                            return record;
                        });
                        return null;
                    }
                })
                .build();

        // Run twice to guard against state leaking across runs
        for (int run: List.of(1, 2)) {
            int i = 0;
            for (JsonNode recordJson : recordsJson) {
                final GenericData.Record record = converter.convertToGenericDataRecord(
                        JsonHelper.serialize(recordJson).getBytes(),
                        avroSchema);
                String recordReformatted = JsonHelper.serialize(JsonHelper.deserialize(record.toString()));
                assertEquals(expectedRecords.get(i), recordReformatted, String.format("Test run %d for %s failed", run, testCaseName));
                i++;
            }
        }
    }
}
