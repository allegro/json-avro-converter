package tech.allegro.schema.json2avro.converter

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.json.JsonSlurper
import spock.lang.Specification

class BaseConverterSpec extends Specification {

    def converter = new JsonAvroConverter(new ObjectMapper(),
            {name, value, path -> println "Unknown field $path with value $value"})
    def converterFailOnUnknown = new JsonAvroConverter(new ObjectMapper(), new FailOnUnknownField())
    def slurper = new JsonSlurper()

}
