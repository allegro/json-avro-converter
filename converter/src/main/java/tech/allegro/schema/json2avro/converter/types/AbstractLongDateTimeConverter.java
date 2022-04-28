package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.Schema;

public abstract class AbstractLongDateTimeConverter extends AbstractDateTimeConverter {

    @Override
    protected Object convertNumber(Number numberValue) {
        return numberValue.longValue();
    }

    @Override
    protected Schema.Type getUnderlyingSchemaType() {
        return Schema.Type.LONG;
    }

}
