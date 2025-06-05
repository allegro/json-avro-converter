package tech.allegro.schema.json2avro.converter

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.json.JsonSlurper
import spock.lang.Specification

class BaseConverterSpec extends Specification {

    def avroConverter = new JsonAvroConverter(new ObjectMapper(),
            {name, value, path -> println "Unknown field $path with value $value"})
    def jsonConverter = new AvroJsonConverter()
    def converterFailOnUnknown = new JsonAvroConverter(new ObjectMapper(), new FailOnUnknownField())
    def slurper = new JsonSlurper()

    def toMap(byte[] json) {
        slurper.parse(json)
    }

    def toMap(String json) {
        slurper.parseText(json)
    }
}
