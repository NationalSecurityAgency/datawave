package datawave.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.data.type.NoOpType;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.JsonDeserializer;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.JsonDocumentSerializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.util.QueryStopwatch;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class TestMe {

    private Document docGenerator(int attributes){
        Document doc = new Document();
        if (attributes > 1) {
            for (int i = 0; i < attributes - 1; i++) {
                TypeAttribute<?> newTypeAttr = new TypeAttribute<>(new NoOpType(RandomStringUtils.randomAlphabetic(10)), new Key("shard"), true);
                doc.put(RandomStringUtils.randomAlphabetic(10), newTypeAttr);
            }
        }
        Content newTypeAttr = new Content(RandomStringUtils.randomAlphabetic(10),new Key("shard"),true);
        doc.put(RandomStringUtils.randomAlphabetic(10), newTypeAttr);
        return doc;
    }

    QueryStopwatch runTest(int docCount,int attributeCount, String name, SerializeMe ser, DeSerializeMe deser) throws IOException {
        final List<Document> documents = new ArrayList<>();
        List<byte[]> arrays = new ArrayList<>(docCount);
        QueryStopwatch stopWatch = new QueryStopwatch();
        var initwatch = stopWatch.newStartedStopwatch("Time to generate 100k docs");
        IntStream.range(0,docCount).forEach(x -> {
            arrays.add(null);
            documents.add(docGenerator(attributeCount));
        });
        initwatch.stop();

        var convertStopWatch = stopWatch.newStartedStopwatch("Time to convert " + docCount + " " + name +" docs...serialization");
        for(int i=0;i < documents.size(); i++) {
            arrays.set(i, ser.serialize(documents.get(i)));
        }
        convertStopWatch.stop();
        convertStopWatch = stopWatch.newStartedStopwatch("Time to convert " + docCount + " " + name +" docs...deserialization");
        for(int i=0;i < documents.size(); i++) {
            Document newDoc = deser.deserialize(arrays.get(i));
            Assert.assertEquals(attributeCount,newDoc.size());
        }
        convertStopWatch.stop();

        return stopWatch;
    }

    QueryStopwatch runKryoTest(int docCount, int attributes) throws IOException {
        KryoDocumentSerializer kryoSerializer = new KryoDocumentSerializer();
        KryoDocumentDeserializer kryoDeserializer = new KryoDocumentDeserializer();
        return runTest(docCount,attributes,"kryo",document -> kryoSerializer.serialize(document), bytes -> kryoDeserializer.deserialize(new ByteArrayInputStream(bytes)));
    }

    QueryStopwatch runJSONTest(int docCount, int attributes) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        return runTest(docCount,attributes,"json",document -> {
            try {
                return mapper.writeValueAsBytes(document);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }, bytes -> {
            try {
                return mapper.readValue(bytes,Document.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    QueryStopwatch runGSONTest(int docCount, int attributes) throws IOException {
        JsonDocumentSerializer jsonSerializer = new JsonDocumentSerializer(false);
        JsonDeserializer deserializer = new JsonDeserializer();
        return runTest(docCount,attributes,"gson",document ->jsonSerializer.serialize(document), bytes -> deserializer.deserialize(new ByteArrayInputStream(bytes)));
    }

    @Test
    public void comparisonTest() throws IOException {
        int docCount=50000,attributeCount=3;
        for(int i=0; i < 10; i++) // warmup. can use jmh instead for greater accuracy
            runGSONTest(docCount,attributeCount);
        System.out.println( runGSONTest(docCount,attributeCount).summarize());
        System.out.println( runJSONTest(docCount,attributeCount).summarize());
        System.out.println( runKryoTest(docCount,attributeCount).summarize());

    }

    @FunctionalInterface
    interface SerializeMe{
        byte [] serialize(Document doc);
    }

    @FunctionalInterface
    interface DeSerializeMe{
        Document deserialize(byte [] arr);
    }
}
