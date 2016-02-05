# json-avro-converter

JSON to Avro conversion tool designed to make migration to Avro easier. Includes simple command line validator.

## Motivation

Apache Avro ships with some very advanced and efficient tools for reading and writing binary Avro but their support 
for JSON to Avro conversion is unfortunately limited and requires wrapping fields with type declarations if you have
some optional fields in your schema. This tool is supposed to help with migrating project from using JSON to Avro without
having to modify JSON data if it conforms to the JSON schema.

## JSON2Avro Converter

### Features

* conversion binary JSON to binary Avro
* conversion binary JSON to GenericData.Record
* conversion binary Avro to binary JSON
* optional field support (unions do not require wrapping)
* unknown fields that are not declared in schema are ignored

### Dependencies

```groovy
dependencies {
    compile group: 'tech.allegro.schema.json2avro', name: 'json-avro-converter', version: 'x.x.x'
}
```

### Basic usage

```java
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;

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
byte[] avro = converter.convertJsonToAvro(json.getBytes(), schema);

// conversion to GenericData.Record
GenericData.Record record = converter.convertToGenericDataRecord(json.getBytes(), new Schema.Parser().parse(schema));

// conversion from binary Avro to JSON
byte[] binaryJson = converter.convertToJson(avro, schema);

// exception handling
String invalidJson = "{ \"username\": 8 }";    

try {
    converter.convertJsonToAvro(invalidJson.getBytes(), schema);    
} catch (AvroConversionException ex) {
    // ...
}
```

## Validator

A command line tool for validating your JSON/Avro documents against the schema.

### Build

To bundle the tool into single executable JAR:

```bash
./gradlew :validator:shadowJar
java -jar validator/build/libs/json2avro-validator.jar --help
```

### Usage

Running Validator with `--help` option will print help message listing all possible arguments.
Sample Avro schema and messages can be found in:

* schema: `validator/src/test/resources/user.avcs`
* JSON message: `validator/src/test/resources/user.json`
* Avro message: `validator/src/test/resources/user.avro`

#### JSON to Avro

You can validate your JSON to Avro conversion:
 
```bash
java -jar json2avro-validator.jar -s user.avcs -i user.json
```

If everything will process correctly, the process will end with zero status code.
 
#### Avro to JSON
 
You can convert the Avro binary data into JSON by setting mode ``-m avro2json`` option:
 
```bash
java -jar json2avro-validator.jar -s user.avcs -i user.avro -m avro2json
```

#### JSON to Avro to JSON

If you would like to know how message will look like after encoding and decoding invoke:

```bash
java -jar json2avro-validator.jar -s user.avcs -i user.json -m json2avro2json
```
