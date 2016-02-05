package tech.allegro.schema.json2avro.validator.schema;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.Optional;

public class ValidationResult {

    private Optional<String> output;

    public ValidationResult(Optional<String> output) {
        this.output = output;
    }

    public Optional<String> getOutput() {
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(output);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValidationResult that = (ValidationResult) o;
        return Objects.equals(output, that.output);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("output", output)
                .toString();
    }

    public static ValidationResult success() {
        return new ValidationResult(Optional.empty());
    }

    public static ValidationResult success(String output) {
        return new ValidationResult(Optional.ofNullable(output));
    }
}
