package tech.allegro.schema.json2avro.validator.schema;

import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

class Test {
    public static void main(String[] args) {
        // Avro schema with one string field: username
        String schema =
                "{" +
                        "   \"type\" : \"record\"," +
                        "   \"name\" : \"Acme\"," +
                        "   \"fields\" : [{ \"name\" : \"username\", \"type\" : \"string\" }]" +
                        "}";

        String json = "{ \"username\": \"mike\" }";

        JsonAvroConverter converter = new JsonAvroConverter();

    // conversion to binary Avro
        byte[] avro = converter.convertToAvro(json.getBytes(), schema);

        // Write avro bytes to file named output.avro
        try {
            Files.write(Paths.get("output.avro"), avro);
            System.out.println("Avro data written to output.avro");
        } catch (IOException e) {
            System.err.println("Error writing file: " + e.getMessage());
        }
    }
}