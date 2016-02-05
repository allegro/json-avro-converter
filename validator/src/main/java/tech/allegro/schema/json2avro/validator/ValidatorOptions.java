package tech.allegro.schema.json2avro.validator;

import com.beust.jcommander.Parameter;

public class ValidatorOptions {

    @Parameter(names = {"-s", "--schema"}, description = "Path to the schema file", required = true)
    private String schemaPath;

    @Parameter(names = {"-i", "--input"}, description = "Path to the validated file", required = true)
    private String inputPath;

    @Parameter(names = {"-d", "--debug"}, description = "Enables logging in debug mode")
    private boolean debug = true;

    @Parameter(names = {"-m", "--mode"}, description = "Validation mode. Supported: json2avro, avro2json, json2avro2json")
    private String mode = "json2avro";

    @Parameter(names = "--help", help = true, description = "Displays this help message")
    private boolean help;

    public String getSchemaPath() {
        return schemaPath;
    }

    public void setSchemaPath(String schemaPath) {
        this.schemaPath = schemaPath;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}
