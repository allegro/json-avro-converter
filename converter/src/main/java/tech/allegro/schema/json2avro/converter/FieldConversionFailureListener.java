package tech.allegro.schema.json2avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public abstract class FieldConversionFailureListener {
    /**
     * This is to support behavior like v2 destinations change capture.
     *
     * Specifically, when a field fails to convert:
     *   * the field, change, and reason are added to `_airbyte_metadata.changes[]`
     *   * the field is nulled or truncated
     *
     * At the time of failure, the _airbyte_metadata.changes[] field might
     *   * exist and be empty
     *   * exist and already contain changes
     *   * not have been parsed yet (meta == null)
     *   * have been parsed, but contain a changes field that has not been parsed (meta.changes == null)
     *
     * Therefore, the simplest general feature that will support the desired behavior is
     *   * listener may return a new value for the affected field only
     *   * listener may not mutate any other part of the record on failure
     *   * listener may only push post-processing actions for the record (after required fields definitely exist)
     *
     */

    private final List<Function<GenericData.Record, GenericData.Record>> postProcessingActions = new LinkedList<>();

    protected final void pushPostProcessingAction(Function<GenericData.Record, GenericData.Record> action) {
        postProcessingActions.add(action);
    }

    public abstract Object onFieldConversionFailure(String avroName,
                                                    String originalName,
                                                    Schema schema,
                                                    Object value,
                                                    String path,
                                                    Exception exception);

    public final GenericData.Record flushPostProcessingActions(GenericData.Record record) {
        for (Function<GenericData.Record, GenericData.Record> action : postProcessingActions) {
            record = action.apply(record);
        }
        postProcessingActions.clear();
        return record;
    }

    public final void reset() {
        postProcessingActions.clear();
    }
}
