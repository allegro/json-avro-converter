# json-avro-converter

[![Java CI](https://github.com/airbytehq/json-avro-converter/actions/workflows/java_ci.yaml/badge.svg)](https://github.com/airbytehq/json-avro-converter/actions/workflows/java_ci.yaml)

This is a Json to Avro object converter used by Airbyte. It is based on [json-avro-converter](https://github.com/allegro/json-avro-converter) from [Allegro Tech](https://github.com/allegro). For basic usage, see the README in the original repo.

## Airbyte Specific Features

### Fluent Constructor
The construction of `JsonAvroConverter` and `JsonGenericRecordReader` is now done with a builder.

```java
JsonAvroConverter converter = JsonAvroConverter.builder()
    .setUnknownFieldListener(listener)
    .build();
```

### Name Transformer
According to Avro spec, Avro names must start with `[A-Za-z_]` followed by `[A-Za-z0-9_]` (see [here](https://avro.apache.org/docs/current/spec.html#names) for details), while Json object property names can be more flexible. A name transformer can be set on `JsonAvroConverter` during the construction to convert a Json object property name to a legal Avro name.

```java
Function<String, String> nameTransformer = String::toUpperCase;
JsonGenericRecordReader reader = JsonGenericRecordReader.builder()
    .setNameTransformer(nameTransformer)
    .build();
```

### Additional Properties
A Json object can have additional properties of unknown types, which is not compatible with the Avro schema. So solve this problem during Json to Avro object conversion, we introduced a special field: `_ab_additional_properties` typed as a nullable `map` from string to string:

```json
{
  "name": "_ab_additional_properties",
  "type": ["null", { "type": "map", "values": "string" }],
  "default": null
}
```

When this field exists in the Avro schema for a record, any unknown fields will be serialized as a string be stored under this field.

Given the following Avro schema:

```json
{
  "type": "record",
  "name": "simple_schema",
  "fields": [
    {
      "name": "username",
      "type": ["null", "string"],
      "default": null
    },
    {
      "name": "_ab_additional_properties",
      "type": ["null", { "type": "map", "values": "string" }],
      "default": null
    }
  ]
}
```

this Json object

```json
{
  "username": "Thomas",
  "active": true,
  "age": 21,
  "auth": {
    "auth_type": "ssl",
    "api_key": "abcdefg/012345",
    "admin": false,
    "id": 1000
  }
}
```

will be converted to the following Avro object:

```json
{
  "username": "Thomas",
  "_ab_additional_properties": {
    "active": "true",
    "age": "21",
    "auth": "{\"auth_type\":\"ssl\",\"api_key\":\"abcdefg/012345\",\"admin\":false,\"id\":1000}"
  }
}
```

Note that all fields other than the `username` is moved under `_ab_additional_properties` as a string.

If the Json object already has an `_ab_additional_properties` field, it will follow the same rule:
- If the Avro schema defines an `_ab_additional_properties` field, all subfields inside this field will be kept in the Avro object, but they will all become string. Other unknown fields will also be stored under this field.
- If the Avro schema does not define such field, it will be completely ignored.

For example, with the same Avro schema, given this Json object:

```json
{
  "username": "Thomas",
  "active": true,
  "_ab_additional_properties": {
    "age": 21,
    "auth": {
      "auth_type": "ssl",
      "api_key": "abcdefg/012345",
      "admin": false,
      "id": 1000
    }
  }
}
```

will be converted to the following Avro object:

```json
{
  "username": "Thomas",
  "_ab_additional_properties": {
    "age": "21",
    "active": "true",
    "auth": "{\"auth_type\":\"ssl\",\"api_key\":\"abcdefg/012345\",\"admin\":false,\"id\":1000}"
  }
}
```

Note the undefined `active` field is moved into `_ab_additional_properties`. `age` and `auth` that are originally in `_ab_additional_properties` becomes strings.

## Build
- The build is upgraded to use Java 14 and Gradle 7.2 to match the build environment of Airbyte.
- Maven staging and publishing is removed because they are incompatible with the new build environment.

## License

The original [json-avro-converter](https://github.com/allegro/json-avro-converter) is published under [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
