package tech.allegro.schema.json2avro.validator.test

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class ResourceUtils {

    static Path resource(String path) {

        Paths.get(ResourceUtils.classLoader.getResource(path).toURI())
    }

    static byte[] readResource(String path) {

        Path filePath = resource(path)
        return Files.readAllBytes(filePath)
    }
}