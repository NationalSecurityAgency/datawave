package datawave.query.microbenchmarks;

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import datawave.data.type.NoOpType;
import datawave.marking.MarkingFunctionsFactory;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.serializer.JsonDocumentSerializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.tables.document.batch.DocumentLogic;
import datawave.query.tables.serialization.JsonDocument;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.transformer.JsonDocumentTransformer;
import datawave.query.util.QueryStopwatch;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.result.event.EventBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.TKey;
import org.apache.accumulo.core.dataImpl.thrift.TKeyValue;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * This is a micro microbenchmark attempting to replicate the interaction between the tserver and webserver responses.
 *
 * It is expected that GSON is faster than kryo deserialization because certain liberties can be taken. These same
 *
 * liberties can be made within Kryo, but JSON is a human-readable structure. Serialization is expected to be faster
 *
 * with kryo; however, these serializations can be distributed amongst many tservers.
 */
public class ScannerSerializationToTransformer {

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

    QueryStopwatch runTestWithValue(int docCount,int attributeCount, String name, SerializeMe ser, DeSerializeMe deser) throws IOException {
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
        TKey key = new Key("20220607_1","abcx\u0000efghi","c","d").toThrift();
        for(int i=0;i < documents.size(); i++) {
            TKeyValue tkv = new TKeyValue();
            tkv.setValue(DocumentSerialization.writeBodyWithHeader(ser.serialize(documents.get(i)),0));
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

            EventBase newDoc = deser.deserialize(ntkv);
            Assert.assertEquals(attributeCount,newDoc.getFields().size());
        }
        convertStopWatch.stop();

        return stopWatch;
    }

    QueryStopwatch runTestWithoutValue(int docCount,int attributeCount, String name, SerializeMe ser, JsonDeSerializeMe deser) throws IOException {
        final List<Document> documents = new ArrayList<>();
        List<JsonDocument> arrays = new ArrayList<>(docCount);
        QueryStopwatch stopWatch = new QueryStopwatch();
        var initwatch = stopWatch.newStartedStopwatch("Time to generate 100k docs");
        IntStream.range(0,docCount).forEach(x -> {
            arrays.add(null);
            documents.add(docGenerator(attributeCount));
        });
        initwatch.stop();

        var convertStopWatch = stopWatch.newStartedStopwatch("Time to convert " + docCount + " " + name +" docs...serialization");
        TKey key = new Key("20220607_1","abcx\u0000efghi","c","d").toThrift();
        JsonParser parser = new JsonParser();
        for(int i=0;i < documents.size(); i++) {
            JsonObject jsonObject =parser.parse(new InputStreamReader(new ByteArrayInputStream(ser.serialize(documents.get(i))))).getAsJsonObject();
            arrays.set(i, new JsonDocument(jsonObject,key,new byte[0],25));
        }
        convertStopWatch.stop();
        convertStopWatch = stopWatch.newStartedStopwatch("Time to convert " + docCount + " " + name +" docs...deserialization");
        for(int i=0;i < documents.size(); i++) {
            EventBase newDoc = deser.deserialize(arrays.get(i));
            Assert.assertEquals(attributeCount,newDoc.getFields().size());
        }
        convertStopWatch.stop();

        return stopWatch;
    }

    QueryStopwatch runKryoTest(int docCount, int attributes) throws IOException {
        KryoDocumentSerializer kryoSerializer = new KryoDocumentSerializer();
        Query queryObj = new QueryImpl();
        queryObj.setQueryAuthorizations("A");
        DocumentTransformer transformer = new DocumentTransformer("shard", queryObj, MarkingFunctionsFactory.createMarkingFunctions(), new DefaultResponseObjectFactory(),false);
        return runTestWithValue(docCount,attributes,"kryo",document -> kryoSerializer.serialize(document), tkv -> {
            Map.Entry<Key,Value> kv = Maps.immutableEntry(new Key(tkv.getKey()),new Value(tkv.getValue()));
            return transformer.transform(kv);
        });
    }


    QueryStopwatch runGSONTest(int docCount, int attributes) throws IOException {
        JsonDocumentSerializer jsonSerializer = new JsonDocumentSerializer(false);
        Query queryObj = new QueryImpl();
        queryObj.setQueryAuthorizations("A");
        DocumentLogic logic = new DocumentLogic();

        JsonDocumentTransformer transformer = new JsonDocumentTransformer(logic, queryObj, MarkingFunctionsFactory.createMarkingFunctions(), new DefaultResponseObjectFactory(),false);
        return runTestWithoutValue(docCount,attributes,"gson",document ->jsonSerializer.serialize(document), doc -> {
            return transformer.transform(doc);
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
        EventBase deserialize(TKeyValue arr);
    }

    @FunctionalInterface
    interface JsonDeSerializeMe{
        EventBase deserialize(JsonDocument arr);
    }



}
