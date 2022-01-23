package tech.allegro.schema.json2avro.validator;

import ch.qos.logback.classic.Level;
import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.allegro.schema.json2avro.validator.schema.ValidationMode;
import tech.allegro.schema.json2avro.validator.schema.ValidationOutput;
import tech.allegro.schema.json2avro.validator.schema.ValidatorException;
import tech.allegro.schema.json2avro.validator.schema.Validators;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ValidatorRunner {

    private static final Logger log = LoggerFactory.getLogger(ValidatorRunner.class);

    public static void run(ValidatorOptions options) {

        try {
            Validators.avro()
                    .withMode(getMode(options))
                    .withInput(readFile(options.getInputPath()))
                    .withSchema(readFile(options.getSchemaPath()))
                    .withOutput(getOutput(options.getOutputPath()))
                    .build()
                    .validate();
        } catch (IOException e) {
            throw new ValidatorException("Unexpected error occurred during I/O operation", e);
        }
    }

    public static void main(String[] args) {

        try {
            ValidatorOptions options = parseCommandLine(args);
            configureLogging(options);
            ValidatorRunner.run(options);
        } catch (ValidatorException e) {
            log.error("Document could not be validated", e);
            System.exit(1);
        }
    }

    private static ValidationMode getMode(ValidatorOptions options) {
        return ValidationMode.from(options.getMode());
    }

    private static byte[] readFile(Path path) throws IOException {
        return Files.readAllBytes(path);
    }

    private static ValidationOutput getOutput(Path outputPath) {
        return outputPath != null ? new FileValidationOutput(outputPath) : ValidationOutput.NO_OUTPUT;
    }

    private static void configureLogging(ValidatorOptions options) {

        if (isDebugEnabled(options)) {
            ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.DEBUG);
        }
    }

    private static boolean isDebugEnabled(ValidatorOptions options) {

        return options.isDebug()
                && LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) instanceof ch.qos.logback.classic.Logger;
    }

    private static ValidatorOptions parseCommandLine(String[] args) {
        ValidatorOptions options = new ValidatorOptions();

        JCommander commander = new JCommander(options);
        commander.parse(args);

        commander.setProgramName("java -jar json2avro-validator.jar");

        if (options.isHelp()) {
            commander.usage();
            System.exit(0);
        }
        return options;
    }
}
