package tech.allegro.schema.json2avro.validator.test

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory

class AvroUtils {

    static byte[] recordToBytes(GenericRecord record, Schema schema) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}