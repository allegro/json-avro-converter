package tech.allegro.schema.json2avro.validator.schema;

import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

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

        System.out.println("Binary Avro: " + avro.length);
    }
}