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

### Enforced String Fields

When a Json array field has no `items`, the element of that array field may have any type. However, Avro requires that each array has a clear type specification. To solve this problem, the [Json to Avro schema converter](https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/destination-s3/src/main/java/io/airbyte/integrations/destination/s3/avro/JsonToAvroSchemaConverter.java) assigns `string` as the type of the elements for any untyped arrays. Accordingly, this Json to Avro object converter follows the `string` schema, and any non-null field value is forced to be a string.

For example, given the following Json schema:

```json
{
  "type": "object",
  "properties": {
    "identifier": {
      "type": "array"
    }
  }
}
```

and Json object:

```json
{
  "identifier": ["151", 152, true, { "id": 153 }, null]
}
```

the corresponding Avro schema is:

```json
{
  "type": "record",
  "fields": [
    {
      "name": "identifier",
      "type": [
        "null",
        {
          "type": "array",
          "items": ["null", "string"]
        }
      ],
      "default": null
    }
  ]
}
```

and the Avro object is:

```json
{
  "identifier": ["151", "152", "true", "{\"id\": 153}", null]
}
```

Note that every non-null element inside the `identifier` array field is converted to string.

This enforcement only applies to an array whose items can only be nullable strings. For arrays with union item types, no string enforcing will be carried out.

### Additional Properties

#### Avro Additional Properties
A Json object can have additional properties of unknown types, which is not compatible with the Avro schema. To solve this problem during Json to Avro object conversion, we introduce a special field: `_airbyte_additional_properties` typed as a nullable `map` from `string` to `string`:

```json
{
  "name": "_airbyte_additional_properties",
  "type": ["null", { "type": "map", "values": "string" }],
  "default": null
}
```

The name of this field is customizable:

```java
JsonAvroConverter converter = JsonAvroConverter.builder()
    .setAvroAdditionalPropsFieldName("_additional_properties")
    .build();
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
      "name": "_airbyte_additional_properties",
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
  "_airbyte_additional_properties": {
    "active": "true",
    "age": "21",
    "auth": "{\"auth_type\":\"ssl\",\"api_key\":\"abcdefg/012345\",\"admin\":false,\"id\":1000}"
  }
}
```

Note that all fields other than the `username` is moved under `__airbyte_additional_properties` as a string.

#### Json Additional Properties

The input Json object may also have some fields with unknown types. For example, if the Json object already has an `_airbyte_additional_properties` field, it will follow the same rule:
- If the Avro schema defines an `_airbyte_additional_properties` field, all subfields inside this field will be kept in the Avro object, but they will all become string. Other unknown fields will also be stored under this field.
- If the Avro schema does not define such field, it will be completely ignored.

For example, with the same Avro schema, given this Json object:

```json
{
  "username": "Thomas",
  "active": true,
  "_airbyte_additional_properties": {
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
  "_airbyte_additional_properties": {
    "age": "21",
    "active": "true",
    "auth": "{\"auth_type\":\"ssl\",\"api_key\":\"abcdefg/012345\",\"admin\":false,\"id\":1000}"
  }
}
```

Note the undefined `active` field is moved into `_airbyte_additional_properties`. `age` and `auth` that are originally in `_airbyte_additional_properties` becomes strings.

The name of the additional properties field in the Json object is also customizable:

```java
JsonAvroConverter converter = JsonAvroConverter.builder()
    .setJsonAdditionalPropsFieldNames(Set.of("_additional_properties"))
    .build();
```

By default, both `_ab_additional_properties` and `_airbyte_additional_properties` are the additional properties field names on the Json object.

## Build
- The build is upgraded to use Java 14 and Gradle 7.2 to match the build environment of Airbyte.
- Maven staging and publishing is removed because they are incompatible with the new build environment.

## License

The original [json-avro-converter](https://github.com/allegro/json-avro-converter) is published under [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
