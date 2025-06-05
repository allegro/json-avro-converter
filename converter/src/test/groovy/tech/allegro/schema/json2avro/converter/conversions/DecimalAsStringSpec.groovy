package tech.allegro.schema.json2avro.converter.conversions

import org.apache.avro.LogicalTypes
import tech.allegro.schema.json2avro.converter.BaseConverterSpec

import static java.math.RoundingMode.UNNECESSARY
import static java.nio.ByteBuffer.wrap

class DecimalAsStringSpec extends BaseConverterSpec {

    def logicalType = LogicalTypes.decimal(15, 2)

    def 'should convert to bytes'() {
        given:
            def bigDecimal = new BigDecimal("1234.56").setScale(2, UNNECESSARY)
        when:
            def result = DecimalAsStringConversion.INSTANCE.toBytes(bigDecimal.toPlainString(), null, logicalType)
        then:
            result.array() == '1234.56'.bytes
    }

    def 'should convert from bytes'() {
        given:
            def bigDecimal = new BigDecimal("1234.56").setScale(2, UNNECESSARY)
            def bytes = wrap(bigDecimal.unscaledValue().toByteArray())
        when:
            def result = DecimalAsStringConversion.INSTANCE.fromBytes(bytes, null, logicalType)
        then:
            result == '1234.56'
    }

}
