package tech.allegro.schema.json2avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class JsonAvroConverterBenchmark {

    private JsonAvroConverter converter = new JsonAvroConverter();
    private byte[] messageWithNullField;
    private byte[] completeMessage;
    private Schema schema;

    @Setup
    public void setup() {
        converter = new JsonAvroConverter();
        schema = new Schema.Parser().parse(
                        "{" +
                        "    \"type\" : \"record\"," +
                        "    \"name\" : \"Acme\"," +
                        "    \"fields\" : [" +
                        "        { \"name\" : \"username\", \"type\" : \"string\" }," +
                        "        { \"name\" : \"age\", \"type\" : [\"null\", \"int\"], \"default\": null }]" +
                        "}");

        messageWithNullField = "{ \"username\": \"mike\" }".getBytes();
        completeMessage = "{ \"username\": \"mike\", \"age\": 30}".getBytes();
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public GenericData.Record conversionLatencyForMessageWithNotProvidedOptionalField() {
        return converter.convertToGenericDataRecord(messageWithNullField, schema);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public GenericData.Record conversionThroughputForMessageWithNotProvidedOptionalField() {
        return converter.convertToGenericDataRecord(messageWithNullField, schema);
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public GenericData.Record conversionLatencyForCompleteMessage() {
        return converter.convertToGenericDataRecord(completeMessage, schema);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public GenericData.Record conversionThroughputForCompleteMessage() {
        return converter.convertToGenericDataRecord(completeMessage, schema);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + JsonAvroConverterBenchmark.class.getSimpleName() + ".*")
                .warmupIterations(2)
                .measurementIterations(2)
                .measurementTime(TimeValue.seconds(20))
                .warmupTime(TimeValue.seconds(5))
                .forks(1)
                .threads(1)
                .syncIterations(true)
                .build();

        new Runner(opt).run();
    }

}
