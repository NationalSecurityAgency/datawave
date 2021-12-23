package datawave.query.microbenchmarks;

import com.google.gson.JsonObject;
import datawave.data.type.NoOpType;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.serializer.JsonDocumentSerializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.tables.document.batch.DocumentKeyConversion;
import datawave.query.util.QueryStopwatch;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.TKey;
import org.apache.accumulo.core.dataImpl.thrift.TKeyValue;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * This is a micro microbenchmark attempting to replicate the interaction between the tserver and webserver.
 *
 * In many cases the serialization is faster in kryo. This is becaues kryo is doing a single serialization, where as
 *
 * json incurs a document pojo -> JSON object conversion during serialization. Eliminating this would further improve
 *
 * performance. The value of this microbenchmark is showing that deser is faster with json when we use the JSON object
 *
 * as the return from the tserver. Further, the serialization step can occur on 1 to many tservers, so the difference
 *
 * in time is minimal. Documents still must be converted into webserver responses, as the JSON could be curated as
 *
 * the final response object from the webserver.
 */
public class ScannerSerialization {

    private Document docGenerator(int attributes){
        Document doc = new Document();
        if (attributes > 1) {
            for (int i = 0; i < attributes - 1; i++) {
                TypeAttribute<?> newTypeAttr = new TypeAttribute<>(new NoOpType(RandomStringUtils.randomAlphabetic(10)), new Key("shard"), true);
                doc.put(RandomStringUtils.randomAlphabetic(10), newTypeAttr);
            }
        }
        TypeAttribute<?> newTypeAttr = new TypeAttribute<>(new NoOpType(RandomStringUtils.randomAlphabetic(10)),new Key("shard"),true);
        doc.put(RandomStringUtils.randomAlphabetic(10), newTypeAttr);
        return doc;
    }

    QueryStopwatch runTestWithValue(int docCount, int attributeCount, String name, SerializeMe ser, DeSerializeMe deser, boolean writeIdentifier) throws IOException {
        final List<Document> documents = new ArrayList<>();
        List<TKeyValue> arrays = new ArrayList<>(docCount);
        QueryStopwatch stopWatch = new QueryStopwatch();
        var initwatch = stopWatch.newStartedStopwatch("Time to generate 100k docs");
        IntStream.range(0,docCount).forEach(x -> {
            arrays.add(null);
            documents.add(docGenerator(attributeCount));
        });
        initwatch.stop();

        var convertStopWatch = stopWatch.newStartedStopwatch("Time to convert " + docCount + " " + name +" docs...serialization");
        TKey key = new Key("a","b","c","d").toThrift();
        for(int i=0;i < documents.size(); i++) {
            TKeyValue tkv = new TKeyValue();
            tkv.setValue(DocumentSerialization.writeBodyWithHeader(ser.serialize(documents.get(i)),0,writeIdentifier));
            tkv.setKey(key);
            arrays.set(i, tkv);
        }
        convertStopWatch.stop();
        convertStopWatch = stopWatch.newStartedStopwatch("Time to convert " + docCount + " " + name +" docs...deserialization");
        for(int i=0;i < documents.size(); i++) {
            TKeyValue tkv = arrays.get(i);
            Value value = new Value(tkv.value);
            TKeyValue ntkv = new TKeyValue();
            ntkv.setKey(tkv.getKey());
            ntkv.setValue(tkv.value);

            Document newDoc = deser.deserialize(ntkv);
            Assert.assertEquals(attributeCount,newDoc.size());
        }
        convertStopWatch.stop();

        return stopWatch;
    }

    QueryStopwatch runTestWithoutValue(int docCount,int attributeCount, String name, SerializeMe ser, JsonDeSerializeMe deser) throws IOException {
        final List<Document> documents = new ArrayList<>();
        List<TKeyValue> arrays = new ArrayList<>(docCount);
        QueryStopwatch stopWatch = new QueryStopwatch();
        var initwatch = stopWatch.newStartedStopwatch("Time to generate 100k docs");
        IntStream.range(0,docCount).forEach(x -> {
            arrays.add(null);
            documents.add(docGenerator(attributeCount));
        });
        initwatch.stop();

        var convertStopWatch = stopWatch.newStartedStopwatch("Time to convert " + docCount + " " + name +" docs...serialization");
        TKey key = new Key("a","b","c","d").toThrift();
        for(int i=0;i < documents.size(); i++) {
            TKeyValue tkv = new TKeyValue();
            tkv.setKey(key);
            tkv.setValue(DocumentSerialization.writeBodyWithHeader(ser.serialize(documents.get(i)),0,true));
            arrays.set(i, tkv);
        }
        convertStopWatch.stop();
        convertStopWatch = stopWatch.newStartedStopwatch("Time to convert " + docCount + " " + name +" docs...deserialization");
        for(int i=0;i < documents.size(); i++) {
            JsonObject newDoc = deser.deserialize(arrays.get(i));
            Assert.assertEquals(attributeCount,newDoc.entrySet().size());
        }
        convertStopWatch.stop();

        return stopWatch;
    }

    QueryStopwatch runKryoTest(int docCount, int attributes) throws IOException {
        KryoDocumentSerializer kryoSerializer = new KryoDocumentSerializer();
        return runTestWithValue(docCount,attributes,"kryo",document -> kryoSerializer.serialize(document), tkv -> {
            return DocumentKeyConversion.getDocument(DocumentSerialization.ReturnType.kryo,false,tkv).getAsDocument();
        },false);
    }


    QueryStopwatch runGSONTest(int docCount, int attributes) throws IOException {
        JsonDocumentSerializer jsonSerializer = new JsonDocumentSerializer(false);
        return runTestWithoutValue(docCount,attributes,"gson",document ->jsonSerializer.serialize(document), tkv -> {
            return DocumentKeyConversion.getDocument(DocumentSerialization.ReturnType.json,false,tkv).get();
        });
    }

    @Test
    public void comparisonTest() throws IOException {
        int docCount=50000,attributeCount=5;
        for(int i=0; i < 10; i++) // warmup. can use jmh instead for greater accuracy
            runGSONTest(docCount,attributeCount);
        System.out.println( runKryoTest(docCount,attributeCount).summarize());
        System.out.println( runGSONTest(docCount,attributeCount).summarize());
    }

    @FunctionalInterface
    interface SerializeMe{
        byte [] serialize(Document doc);
    }

    @FunctionalInterface
    interface DeSerializeMe{
        Document deserialize(TKeyValue arr);
    }

    @FunctionalInterface
    interface JsonDeSerializeMe{
        JsonObject deserialize(TKeyValue  arr);
    }

}
