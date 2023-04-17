package datawave.query.attributes;

import com.google.common.collect.Maps;
import datawave.data.type.*;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.jts.io.WKTReader;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DocumentSerializationTimingIT {
    @State(Scope.Thread)
    public static class SerializationState {
        private final static int DOCUMENT_SAMPLES = 10000;
        private final static int ATTRIBUTE_COUNT = 50;
        private final static int ATTRIBUTE_ARRAY_COUNT = 4;
        private final static int ATTRIBUTE_DATE_COUNT = 4;
        private final static byte[] EMPTY_BYTES = new byte[] {};
        
        private KryoDocumentDeserializer deserializer;
        private Collection<Map.Entry<Key,Value>> serializedKv;
        
        @Setup
        public void setup() {
            deserializer = new KryoDocumentDeserializer();
            
            KryoDocumentSerializer serializer = new KryoDocumentSerializer(true, true);
            Key key = new Key(EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, new ColumnVisibility("SOMEVAL|(OTHER&CAR&BIRD&(A|B|C))"), System.currentTimeMillis());
            Random random = new Random(42);
            RandomStringGenerator rg = new RandomStringGenerator.Builder().usingRandom(random::nextInt).withinRange('a', 'z').build();
            RandomStringGenerator rh = new RandomStringGenerator.Builder().usingRandom(random::nextInt).withinRange('a', 'f').build();
            serializedKv = IntStream
                            .range(0, DOCUMENT_SAMPLES)
                            .mapToObj(idx -> {
                                Document doc = new Document();
                                try {
                                    TimingMetadata timing = new TimingMetadata();
                                    timing.setHost("localhost");
                                    timing.setMetadata(key);
                                    timing.setNextCount(100);
                                    timing.setSeekCount(1000);
                                    timing.setSourceCount(100);
                                    timing.setYieldCount(100);
                                    timing.addStageTimer("STAGE_1", new Numeric("3", key, true));
                                    timing.addStageTimer("STAGE_2", new Numeric("100", key, true));
                                    timing.addStageTimer("STAGE_3", new Numeric("300", key, true));
                                    IntStream.range(0, ATTRIBUTE_COUNT).forEach(
                                                    fidx -> doc.put(rg.generate(3, 8), new TypeAttribute<>(new LcNoDiacriticsType(rg.generate(5, 15)), key,
                                                                    true)));
                                    IntStream.range(0, ATTRIBUTE_ARRAY_COUNT).forEach(
                                                    fidx -> doc.put(rg.generate(3, 8),
                                                                    new Attributes(IntStream.range(0, 2)
                                                                                    .mapToObj(aidx -> new Content(rg.generate(5, 15), key, true))
                                                                                    .collect(Collectors.toList()), true)));
                                    IntStream.range(0, ATTRIBUTE_DATE_COUNT).forEach(
                                                    fidx -> doc.put(rg.generate(3, 8), new TypeAttribute<>(new DateType(Date.from(Instant.now()).toString()),
                                                                    key, true)));
                                    DateType d1 = new DateType("2023-03-01 00:24:21Z");
                                    DateType d2 = new DateType("2023-03-21 12:24:21Z");
                                    DateType d3 = new DateType("2023-04-05 18:24:21Z");
                                    DateType d4 = new DateType("2023-06-01 11:24:21Z");
                                    DateType d5 = new DateType("2023-02-01 12:24:21Z");
                                    HexStringType h1 = new HexStringType();
                                    HexStringType h2 = new HexStringType();
                                    HexStringType h3 = new HexStringType();
                                    GeometryType g1 = new GeometryType();
                                    GeometryType g2 = new GeometryType();
                                    GeometryType g3 = new GeometryType();
                                    GeometryType g4 = new GeometryType();
                                    GeoLatType gg1 = new GeoLatType();
                                    GeoLonType gg2 = new GeoLonType();
                                    IpAddressType ip1 = new IpAddressType();
                                    IpAddressType ip2 = new IpAddressType();
                                    IpAddressType ip3 = new IpAddressType();
                                    g1.setDelegate(new datawave.data.type.util.Geometry(new WKTReader()
                                                    .read("MULTIPOINT(0 0, 1 1, 2 2, 3 3, 4 4, 5 5, 6 6, 7 7, 8 8, 9 9, 10 10)")));
                                    g2.setDelegate(new datawave.data.type.util.Geometry(new WKTReader()
                                                    .read("MULTIPOINT(0 0, 1 1, 2 2, 3 3, 4 4, 5 5, 6 6, 7 7, 8 8, 9 9, 10 10)")));
                                    g3.setDelegate(new datawave.data.type.util.Geometry(new WKTReader()
                                                    .read("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90), (-45 -45, 45 -45, 45 45, -45 45, -45 -45))")));
                                    g4.setDelegate(new datawave.data.type.util.Geometry(new WKTReader()
                                                    .read("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90), (-45 -45, 45 -45, 45 45, -45 45, -45 -45))")));
                                    gg1.setDelegate("55.7558");
                                    gg2.setDelegate("37.6173");
                                    ip1.setDelegateFromString(ip1.normalize(String.format("%d.%d.%d.*", random.nextInt(100), random.nextInt(100), random.nextInt(100))));
                                    ip2.setDelegateFromString(ip2.normalize(String.format("%d.%d.*.*", random.nextInt(100), random.nextInt(100))));
                                    ip3.setDelegateFromString(ip3.normalize(String.format("*.%d.%d.%d", random.nextInt(100), random.nextInt(100), random.nextInt(100))));
                                    h1.setDelegateFromString("0x" + rh.generate(6));
                                    h2.setDelegateFromString("0x" + rh.generate(6));
                                    h3.setDelegateFromString(rh.generate(6));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(g1, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(g1, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(h1, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(g2, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(g3, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(ip1, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(d2, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(g4, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(h2, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(gg1, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(d3, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(ip2, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(h3, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(gg2, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(d4, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(ip3, key, true));
                                    doc.put(rg.generate(3, 8), new TypeAttribute<>(d5, key, true));
                                    doc.put("TIMING_METADATA", timing);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                                Map.Entry<Key,Value> kv = serializer.apply(Maps.immutableEntry(key, doc));
                                return kv;
                            }).collect(Collectors.toList());
        }
    }
    
    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void deserializationBenchmark(Blackhole bh, SerializationState state) {
        for (Map.Entry<Key,Value> kv : state.serializedKv) {
            Map.Entry<Key,Document> drop = state.deserializer.apply(kv);
            bh.consume(drop);
        }
    }
    
    @Test
    @Ignore
    public void runBenchmarks() throws Exception {
        // @formatter:off
        Options options = new OptionsBuilder()
                .include(this.getClass().getName() + ".*")
                .mode(Mode.AverageTime)
                .warmupTime(TimeValue.seconds(1))
                .warmupIterations(3)
                .measurementIterations(3)
                .operationsPerInvocation(SerializationState.DOCUMENT_SAMPLES)
                .threads(1)
                .forks(1)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                .build();
        // formatter:on
        new Runner(options).run();
    }
}
