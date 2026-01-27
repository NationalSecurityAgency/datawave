package datawave.query.function.serializer;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.type.DateType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.util.TypeMetadata;
import datawave.util.time.DateHelper;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DocumentSerializationIT {

    private static final Logger log = LoggerFactory.getLogger(DocumentSerializationIT.class);

    private final int maxIterations = 1_000;
    private final int lowThreads = 2;
    private final int highThreads = 7;

    private AttributeFactory attributeFactory;
    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();

    private final String datatype = "datatype";
    private final ColumnVisibility visibility = new ColumnVisibility("PUBLIC");
    private final Key documentKey = new Key("row", "datatype\0uid", "", visibility, 0L);

    private final AtomicLong count = new AtomicLong(0);

    @BeforeEach
    public void setup() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("FIELD_A", datatype, LcNoDiacriticsType.class.getTypeName());
        metadata.put("FIELD_B", datatype, LcNoDiacriticsType.class.getTypeName());
        metadata.put("FIELD_C", datatype, LcNoDiacriticsType.class.getTypeName());
        metadata.put("NUM", datatype, NumberType.class.getTypeName());
        metadata.put("DATE", datatype, DateType.class.getTypeName());

        attributeFactory = new AttributeFactory(metadata);
    }

    @Test
    @Order(1)
    public void testLowThreadsLowVariance() {
        driveThreads(lowThreads, 10);
    }

    @Test
    @Order(2)
    public void testLowThreadsHighVariance() {
        driveThreads(lowThreads, 100);
    }

    @Test
    @Order(3)
    public void testHighThreadsLowVariance() {
        driveThreads(highThreads, 10);
    }

    @Test
    @Order(4)
    public void testHighThreadsHighVariance() {
        driveThreads(highThreads, 100);
    }

    protected void driveThreads(int threads, int numDocs) {
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        List<byte[]> serializedDocs = createSerializedDocuments(numDocs);
        List<Callable<Long>> callables = getCallables(threads, serializedDocs);

        try {
            long sum = 0L;
            List<Future<Long>> futures = executor.invokeAll(callables);
            for (Future<Long> future : futures) {
                long time = future.get();
                sum += time;
            }

            long timePerDocument = sum / ((long) threads * numDocs);
            timePerDocument = TimeUnit.NANOSECONDS.toMillis(timePerDocument);
            log.info("{} threads, {} docs, {} ms/doc", threads, numDocs, timePerDocument);
        } catch (InterruptedException | ExecutionException e) {
            fail("failed with exception", e);
            throw new RuntimeException(e);
        }
    }

    private List<Callable<Long>> getCallables(int threads, List<byte[]> serializedDocs) {
        List<Callable<Long>> callables = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final List<byte[]> copy = new ArrayList<>(serializedDocs);
            callables.add(() -> {
                long total = 0L;
                KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();
                for (int k = 0; k < maxIterations; k++) {
                    for (byte[] data : copy) {
                        try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
                            long elapsed = System.nanoTime();
                            deserializer.deserialize(bais);
                            total += System.nanoTime() - elapsed;
                        }
                    }
                }
                return total;
            });
        }
        return callables;
    }

    private List<byte[]> createSerializedDocuments(int n) {
        List<byte[]> docs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Document d = createDocument();
            byte[] data = serializer.serialize(d);
            docs.add(data);
        }
        return docs;
    }

    private Document createDocument() {
        Document doc = new Document();
        Set<String> fields = Set.of("FIELD_A", "FIELD_B", "FIELD_C");
        for (String field : fields) {
            for (int i = 0; i < 6; i++) {
                doc.put(field, createAttribute(field, "some random text: " + i));
            }
        }
        for (int i = 0; i < 10; i++) {
            Content content = new Content("token " + i, documentKey, true);
            doc.put("CONTENT", content);
        }
        doc.put("NUM", createAttribute("NUM", "23"));
        doc.put("DATE", createAttribute("DATE", DateHelper.format(System.currentTimeMillis())));

        String cf = "datatype\0uid-" + count.incrementAndGet();
        Key metadata = new Key("row", cf, "", visibility, 0L);
        doc.put(Document.DOCKEY_FIELD_NAME, new DocumentKey(metadata, true));
        return doc;
    }

    protected Attribute<?> createAttribute(String field, String value) {
        return attributeFactory.create(field, value, documentKey, datatype, true);
    }
}
