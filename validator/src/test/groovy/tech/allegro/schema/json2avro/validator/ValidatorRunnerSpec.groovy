package tech.allegro.schema.json2avro.validator

import spock.lang.Specification
import spock.lang.Subject
import tech.allegro.schema.json2avro.validator.schema.ValidatorException

import static tech.allegro.schema.json2avro.validator.test.ResourceUtils.resource

class ValidatorRunnerSpec extends Specification {

    @Subject
    ValidatorRunner runner

    void setup() {

        runner = new ValidatorRunner()
    }

    def "should validate JSON document against the schema"() {

        given:
        ValidatorOptions options = new ValidatorOptions(schemaPath: resource('user.avcs'), inputPath: resource('user.json'))

        when:
        runner.run(options)

        then:
        noExceptionThrown()
    }

    def "should report JSON validation errors"() {

        given:
        ValidatorOptions options = new ValidatorOptions(schemaPath: resource('user.avcs'),
                inputPath: resource('invalid_user.json'))

        when:
        runner.run(options)

        then:
        thrown(ValidatorException.class)
    }

    def "should validate AVRO document against the schema"() {

        given:
        ValidatorOptions options = new ValidatorOptions(schemaPath: resource('user.avcs'), inputPath: resource('user.avro'), mode: 'avro2json')

        when:
        runner.run(options)

        then:
        noExceptionThrown()
    }
}
