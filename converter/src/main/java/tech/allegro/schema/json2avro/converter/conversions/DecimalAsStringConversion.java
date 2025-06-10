package tech.allegro.schema.json2avro.converter.conversions;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.nio.ByteBuffer;

public class DecimalAsStringConversion extends Conversion<String> {
    public static final DecimalAsStringConversion INSTANCE = new DecimalAsStringConversion();
    private final Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();

    private DecimalAsStringConversion() {}

    @Override
    public Class<String> getConvertedType() {
        return String.class;
    }

    @Override
    public String getLogicalTypeName() {
        return "decimal";
    }

    @Override
    public String fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
        return decimalConversion.fromBytes(value, schema, type).toPlainString();
    }

    @Override
    public ByteBuffer toBytes(String value, Schema schema, LogicalType type) {
        return ByteBuffer.wrap(value.getBytes());
    }
}
