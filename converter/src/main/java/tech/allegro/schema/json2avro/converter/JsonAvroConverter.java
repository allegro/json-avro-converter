package tech.allegro.schema.json2avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.NoWrappingJsonEncoder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class JsonAvroConverter {
    private JsonGenericRecordReader recordReader;

    public JsonAvroConverter() {
        this.recordReader = new JsonGenericRecordReader();
    }

    public JsonAvroConverter(ObjectMapper objectMapper) {
        this.recordReader = new JsonGenericRecordReader(objectMapper);
    }

    public byte[] convertToAvro(byte[] data, String schema) {
        return convertToAvro(data, new Schema.Parser().parse(schema));
    }

    public byte[] convertToAvro(byte[] data, Schema schema) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
            writer.write(convertToGenericDataRecord(data, schema), encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new AvroConversionException("Failed to convert to AVRO.", e);
        }
    }

    public GenericData.Record convertToGenericDataRecord(byte[] data, Schema schema) {
        return recordReader.read(data, schema);
    }

    public byte[] convertToJson(byte[] avro, String schema) {
        return convertToJson(avro, new Schema.Parser().parse(schema));
    }

    public byte[] convertToJson(byte[] avro, Schema schema) {
        try {
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(avro, null);
            GenericRecord record = new GenericDatumReader<GenericRecord>(schema).read(null, binaryDecoder);
            return convertToJson(record);
        } catch (IOException e) {
            throw new AvroConversionException("Failed to create avro structure.", e);
        }
    }

    public byte[] convertToJson(GenericRecord record) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            NoWrappingJsonEncoder jsonEncoder = new NoWrappingJsonEncoder(record.getSchema(), outputStream);
            new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, jsonEncoder);
            jsonEncoder.flush();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new AvroConversionException("Failed to convert to JSON.", e);
        }
    }
}
