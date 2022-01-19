package tech.allegro.schema.json2avro.validator;

import tech.allegro.schema.json2avro.validator.schema.ValidationOutput;
import tech.allegro.schema.json2avro.validator.schema.ValidatorException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class FileValidationOutput implements ValidationOutput {

    private final Path outputPath;

    FileValidationOutput(Path outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void write(byte[] output) {
        try {
            Files.write(outputPath, output);
        } catch (IOException e) {
            throw new ValidatorException("Error occurred when writing the output", e);
        }
    }
}
