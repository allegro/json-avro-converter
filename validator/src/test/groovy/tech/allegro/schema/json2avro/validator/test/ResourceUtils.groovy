package tech.allegro.schema.json2avro.validator.test

import java.nio.file.Files
import java.nio.file.Paths

class ResourceUtils {

    static String resource(String path) {

        new File(ResourceUtils.classLoader.getResource(path).toURI()).getAbsolutePath()
    }

    static byte[] readResource(String path) {

        String filePath = resource(path)
        return Files.readAllBytes(Paths.get(filePath))
    }
}