package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Deque;

import static org.apache.avro.Schema.Type.BYTES;
import static tech.allegro.schema.json2avro.converter.PathsPrinter.print;

public class BytesDecimalConverter implements AvroTypeConverter {
    public static final AvroTypeConverter INSTANCE = new BytesDecimalConverter();

    private BytesDecimalConverter() {

    }

    @Override
    public Object convert(Schema.Field field, Schema schema, Object value, Deque<String> path, boolean silently) {
        try {
            int scale = (int) schema.getObjectProp("scale");
            return convertDecimal(value, scale);
        } catch (NumberFormatException exception) {
            if (silently) {
                return new Incompatible("string number, decimal");
            } else {
                throw new AvroTypeException("Field " + print(path) + " is expected to be a valid number. current value is " + value + ".");
            }
        }
    }

    protected Object convertDecimal(Object value, int scale) {
        BigDecimal bigDecimal = bigDecimalWithExpectedScale(value.toString(), scale);
        return ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray());
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> deque) {
        return BYTES.equals(schema.getType())
                && AvroTypeConverter.isLogicalType(schema, "decimal")
                && schema.getObjectProp("scale") != null;
    }

    protected BigDecimal bigDecimalWithExpectedScale(String decimal, int scale) {
        BigDecimal bigDecimalInput = new BigDecimal(decimal);
        return bigDecimalInput.setScale(scale, RoundingMode.DOWN);
    }
}
