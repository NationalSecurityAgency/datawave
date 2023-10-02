package datawave.query.transformer;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.marking.MarkingFunctions;
import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.query.tables.SSDeepSimilarityQueryLogic;
import datawave.query.util.ssdeep.ChunkSizeEncoding;
import datawave.query.util.ssdeep.IntegerEncoding;
import datawave.query.util.ssdeep.NGramTuple;
import datawave.query.util.ssdeep.SSDeepHash;
import datawave.webservice.query.Query;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*"})
public class SSDeepSimilarityQueryTransformerTest {
    @Mock
    private Query mockQuery;

    @Mock
    private MarkingFunctions mockMarkingFunctions;

    @Mock
    private ResponseObjectFactory mockResponseFactory;

    String chunk = "//thPkK";
    int chunkSize = 3;
    String ssdeepString = "3:yionv//thPkKlDtn/rXScG2/uDlhl2UE9FQEul/lldDpZflsup:6v/lhPkKlDtt/6TIPFQEqRDpZ+up";

    public void basicExpects(Key k) {
        EasyMock.expect(mockQuery.getQueryAuthorizations()).andReturn("A,B,C");
        EasyMock.expect(mockResponseFactory.getEventQueryResponse()).andReturn(new DefaultEventQueryResponse());
        EasyMock.expect(mockResponseFactory.getEvent()).andReturn(new DefaultEvent()).times(1);
        EasyMock.expect(mockResponseFactory.getField()).andReturn(new DefaultField()).times(4);
    }

    @Test
    public void transformTest() {
        int bucketEncodingBase = 32;
        int bucketEncodingLength = 2;

        NGramTuple tuple = new NGramTuple(chunkSize, chunk);
        SSDeepHash hash = SSDeepHash.parse(ssdeepString);

        Multimap<NGramTuple,SSDeepHash> queryMap = TreeMultimap.create();
        queryMap.put(tuple, hash);

        Key key = new Key("+++//thPkK", "3", "3:yionv//thPkKlDtn/rXScG2/uDlhl2UE9FQEul/lldDpZflsup:6v/lhPkKlDtt/6TIPFQEqRDpZ+up");
        Value value = new Value();
        AbstractMap.SimpleEntry<Key,Value> entry = new AbstractMap.SimpleEntry<>(key, value);

        SSDeepSimilarityQueryConfiguration config = SSDeepSimilarityQueryConfiguration.create();
        config.setBucketEncodingBase(bucketEncodingBase);
        config.setBucketEncodingLength(bucketEncodingLength);
        config.setQueryMap(queryMap);

        basicExpects(key);

        PowerMock.replayAll();

        SSDeepSimilarityQueryTransformer transformer = new SSDeepSimilarityQueryTransformer(mockQuery, config, mockMarkingFunctions, mockResponseFactory);
        Map.Entry<SSDeepHash,NGramTuple> transformedTuple = transformer.transform(entry);
        List<Object> resultList = new ArrayList<>();
        resultList.add(transformedTuple);
        BaseQueryResponse response = transformer.createResponse(resultList);

        PowerMock.verifyAll();

        Assert.assertNotNull(transformedTuple);
        Assert.assertEquals(hash, transformedTuple.getKey());
        Assert.assertEquals(tuple, transformedTuple.getValue());
    }
}
