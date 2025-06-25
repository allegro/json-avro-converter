# json-avro-converter

[![Build Status](https://github.com/allegro/json-avro-converter/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/allegro/json-avro-converter/actions/workflows/ci.yml?query=branch%3Amaster)

> **This project is no longer in active development and is currently in maintenance mode.** 
We will no longer actively review issues, merge pull-requests, and release new versions. If you are interested in developing it, please fork this repository.

This project is a JSON to Avro conversion tool designed to make migration to Avro easier. It includes a simple command line validator.

## Motivation

Apache Avro ships with some very advanced and efficient tools for reading and writing binary Avro but their support 
for JSON to Avro conversion is unfortunately limited and requires wrapping fields with type declarations if you have
some optional fields in your schema. This tool is supposed to help with migrating projects from using JSON to Avro without
having to modify JSON data if it conforms to the JSON schema.

## JSON2Avro Converter

### Features

* conversion of binary JSON to binary Avro
* conversion of binary JSON to GenericData.Record
* conversion of binary JSON to Avro generated Java classes
* conversion of binary Avro to binary JSON
* optional field support (unions do not require wrapping)
* unknown fields that are not declared in schema are ignored

### Dependencies

```groovy
dependencies {
    compile group: 'tech.allegro.schema.json2avro', name: 'converter', version: '0.3.0'
}
```

### Basic usage

```java
import tech.allegro.schema.json2avro.converter.AvroConversionException;
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
byte[] avro = converter.convertToAvro(json.getBytes(), schema);

// conversion to GenericData.Record
GenericData.Record record = converter.convertToGenericDataRecord(json.getBytes(), new Schema.Parser().parse(schema));

// exception handling
String invalidJson = "{ \"username\": 8 }";    

try {
    converter.convertToAvro(invalidJson.getBytes(), schema);    
} catch (AvroConversionException ex) {
    System.err.println("Caught exception: " + ex.getMessage());
}

AvroJsonConverter converter = new AvroJsonConverter();

// conversion from binary Avro to JSON
byte[] binaryJson = converter.convertToJson(avro, schema);
```

### Advanced usage

If some avro types are not managed by the library, you can extend it by adding your own AvroTypeConverter. 
An AvroTypeConverter read a json value and convert it to an avro value. This can be useful when some logical-types are missing. 
The AvroTypeConverter can also be used to define a customer converter for a specific path. 

```java
public class CustomFieldConverter implements AvroTypeConverter {
    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        return "custom-" + jsonValue;
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> path) {
        return "customField".equals(path.getLast());
    }
}
```

To use the converter you should add it to the `JsonAvroConverter`, to do that you should build it like that
```java
new JsonAvroConverter(new CompositeJsonToAvroReader(new CustomFieldConverter()))

new AvroJsonConverter()
```

## Validator

A command line tool for validating your JSON/Avro documents against a schema.

### Build

To bundle the tool into single executable JAR:

```bash
./gradlew :validator:shadowJar
java -jar validator/build/libs/json2avro-validator-{version}.jar --help
```

### Usage

Running Validator with `--help` option will print a help message listing all possible arguments.
Sample Avro schema and messages can be found in:

* schema: `validator/src/test/resources/user.avcs`
* JSON message: `validator/src/test/resources/user.json`
* Avro message: `validator/src/test/resources/user.avro`

#### JSON to Avro

You can validate your JSON to Avro conversion:
 
```bash
java -jar json2avro-validator.jar -s user.avcs -i user.json
```

If everything processes correctly, the process will end with zero status code.
 
#### Avro to JSON
 
You can convert the Avro binary data into JSON by setting mode ``-m avro2json`` option:
 
```bash
java -jar json2avro-validator.jar -s user.avcs -i user.avro -m avro2json
```

#### JSON to Avro to JSON

If you would like to know how messages will look like after encoding and decoding invoke:

```bash
java -jar json2avro-validator.jar -s user.avcs -i user.json -m json2avro2json
```

## License

**json-avro-converter** is published under [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
