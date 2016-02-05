package tech.allegro.schema.json2avro.validator.schema.avro

import groovy.json.JsonSlurper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import tech.allegro.schema.json2avro.validator.schema.ValidationMode
import tech.allegro.schema.json2avro.validator.schema.ValidatorException
import spock.lang.Specification
import spock.lang.Subject
import tech.allegro.schema.json2avro.validator.schema.ValidationResult
import tech.allegro.schema.json2avro.validator.schema.Validators
import tech.allegro.schema.json2avro.validator.test.AvroUtils

import static tech.allegro.schema.json2avro.validator.test.ResourceUtils.readResource

class AvroValidatorSpec extends Specification {

    @Subject
    AvroValidator validator

    byte[] avroSchema

    Schema schema

    void setup() {

        avroSchema = readResource("user.avcs")
        schema = new Schema.Parser().parse(new ByteArrayInputStream(avroSchema))
    }

    def "should validate JSON document against the schema"() {

        given:
        validator = Validators.avro()
                .withInput(readResource("user.json"))
                .withSchema(avroSchema)
                .build()

        when:
        validator.validate()

        then:
        noExceptionThrown()
    }

    def "should report JSON validation errors"() {

        given:
        validator = Validators.avro()
                .withInput(readResource("invalid_user.json"))
                .withSchema(avroSchema)
                .build()

        when:
        validator.validate()

        then:
        thrown(ValidatorException.class)
    }

    def "should validate AVRO document against the schema"() {

        given:
        GenericRecord user = new GenericData.Record(schema)
        user.put("name", "Bob")
        user.put("age", 50)
        user.put("favoriteColor", "blue")

        and:
        validator = Validators.avro()
                .withMode(ValidationMode.AVRO_TO_JSON)
                .withInput(AvroUtils.recordToBytes(user, schema))
                .withSchema(avroSchema)
                .build()

        when:
        ValidationResult result = validator.validate()

        then:
        noExceptionThrown()
        result.output.isPresent()
    }

    def "should validate conversion from JSON to AVRO and back to JSON"() {

        given:
        validator = Validators.avro()
                .withInput(readResource("user.json"))
                .withMode(ValidationMode.JSON_TO_AVRO_TO_JSON)
                .withSchema(avroSchema)
                .build()

        when:
        ValidationResult result = validator.validate()

        then:
        def json = new JsonSlurper().parseText(result.output.get())

        expect:
        json.name == "Bob"
    }
}
