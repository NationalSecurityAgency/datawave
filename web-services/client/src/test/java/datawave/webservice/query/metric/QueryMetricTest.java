package datawave.webservice.query.metric;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import datawave.marking.MarkingFunctions;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.metric.BaseQueryMetric.Lifecycle;
import datawave.webservice.query.metric.BaseQueryMetric.PageMetric;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

public class QueryMetricTest {
    
    private static QueryMetric queryMetric = null;
    private static Map<String,String> markings = null;
    private static List<String> negativeSelectors = null;
    private static ArrayList<PageMetric> pageTimes = null;
    private static List<String> positiveSelectors = null;
    private static List<String> proxyServers = null;
    
    @BeforeClass
    public static void setup() {
        queryMetric = new QueryMetric();
        markings = new HashMap<>();
        markings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, "PUBLIC");
        queryMetric.setMarkings(markings);
        negativeSelectors = new ArrayList<>();
        negativeSelectors.add("negativeSelector1");
        positiveSelectors = new ArrayList<>();
        positiveSelectors.add("positiveSelector1");
        pageTimes = new ArrayList<>();
        PageMetric pageMetric = new PageMetric();
        pageMetric.setCallTime(0);
        pageTimes.add(pageMetric);
        proxyServers = new ArrayList<>();
        proxyServers.add("proxyServer1");
    }
    
    @Test
    public void testSetError() {
        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FIELDS_NOT_IN_DATA_DICTIONARY, "test");
        Exception e = new Exception(qe);
        
        queryMetric.setError(e);
        assertEquals("The query contained fields which do not exist in the data dictionary for any specified datatype. test", queryMetric.getErrorMessage());
        assertEquals("400-16", queryMetric.getErrorCode());
        
        queryMetric.setErrorCode("");
        Throwable t = new Throwable("non-datawave error");
        queryMetric.setError(t);
        assertEquals("non-datawave error", queryMetric.getErrorMessage());
        assertEquals("", queryMetric.getErrorCode());
    }
    
    @Test
    public void testSettersGetters() {
        Date d = new Date();
        queryMetric.setBeginDate(d);
        queryMetric.setCreateCallTime(0);
        queryMetric.setCreateDate(d);
        queryMetric.setEndDate(d);
        queryMetric.setErrorCode("error");
        queryMetric.setErrorMessage("errorMessage");
        queryMetric.setHost("host");
        queryMetric.setLastUpdated(d);
        queryMetric.setLastWrittenHash(0);
        queryMetric.setLifecycle(Lifecycle.INITIALIZED);
        queryMetric.setMarkings(markings);
        queryMetric.setNegativeSelectors(negativeSelectors);
        queryMetric.setNumUpdates(0);
        queryMetric.setPageTimes(pageTimes);
        queryMetric.setPositiveSelectors(positiveSelectors);
        queryMetric.setProxyServers(proxyServers);
        queryMetric.setQuery("query");
        queryMetric.setQueryAuthorizations("auths");
        queryMetric.setQueryId("queryId");
        queryMetric.setQueryLogic("queryLogic");
        queryMetric.setQueryType(this.getClass());
        queryMetric.setQueryType("queryType");
        queryMetric.setSetupTime(0);
        queryMetric.setUser("user");
        queryMetric.setUserDN("userDN");
        
        assertEquals(d, queryMetric.getBeginDate());
        assertEquals("PUBLIC", queryMetric.getColumnVisibility());
        assertEquals(0, queryMetric.getCreateCallTime());
        assertEquals(d, queryMetric.getCreateDate());
        assertEquals(0, queryMetric.getElapsedTime());
        assertEquals(d, queryMetric.getEndDate());
        assertEquals("error", queryMetric.getErrorCode());
        assertEquals("errorMessage", queryMetric.getErrorMessage());
        assertEquals("host", queryMetric.getHost());
        assertEquals(d, queryMetric.getLastUpdated());
        assertEquals(0, queryMetric.getLastWrittenHash());
        assertEquals(Lifecycle.INITIALIZED, queryMetric.getLifecycle());
        assertEquals("PUBLIC", queryMetric.getMarkings().get(MarkingFunctions.Default.COLUMN_VISIBILITY));
        assertEquals("negativeSelector1", queryMetric.getNegativeSelectors().get(0));
        assertEquals(1, queryMetric.getNumPages());
        assertEquals(0, queryMetric.getNumResults());
        assertEquals(0, queryMetric.getNumUpdates());
        assertEquals(0, queryMetric.getPageTimes().get(0).getCallTime());
        assertEquals("positiveSelector1", queryMetric.getPositiveSelectors().get(0));
        assertEquals("proxyServer1", queryMetric.getProxyServers().iterator().next());
        assertEquals("query", queryMetric.getQuery());
        assertEquals("auths", queryMetric.getQueryAuthorizations());
        assertEquals("queryId", queryMetric.getQueryId());
        assertEquals("queryLogic", queryMetric.getQueryLogic());
        assertEquals("queryType", queryMetric.getQueryType());
        assertEquals(0, queryMetric.getSetupTime());
        assertEquals("user", queryMetric.getUser());
        assertEquals("userDN", queryMetric.getUserDN());
    }
    
    @Test
    public void testJsonSerialization() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JaxbAnnotationModule());
        String metricAsBytes = objectMapper.writeValueAsString(queryMetric);
        QueryMetric deserializedMetric = objectMapper.readValue(metricAsBytes, QueryMetric.class);
        assertEquals(queryMetric, deserializedMetric);
    }
    
    @Test
    public void testXmlSerialization() throws Exception {
        JAXBContext jaxbContext = JAXBContext.newInstance(QueryMetric.class);
        Marshaller marshaller = jaxbContext.createMarshaller();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        marshaller.marshal(queryMetric, baos);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        QueryMetric deserializedMetric = (QueryMetric) unmarshaller.unmarshal(new ByteArrayInputStream(baos.toByteArray()));
        assertEquals(queryMetric, deserializedMetric);
    }
    
    @Test
    public void testProtobufSerialization() throws Exception {
        Schema<QueryMetric> schema = (Schema<QueryMetric>) queryMetric.getSchemaInstance();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ProtostuffIOUtil.writeTo(baos, queryMetric, schema, LinkedBuffer.allocate());
        QueryMetric deserializedMetric = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(baos.toByteArray(), deserializedMetric, schema);
        assertEquals(queryMetric, deserializedMetric);
    }
    
    @Test
    public void testVersionSerialization() throws Exception {
        QueryMetric qm = new QueryMetric();
        Date d = new Date();
        qm.setBeginDate(d);
        qm.setCreateCallTime(0);
        qm.setCreateDate(d);
        qm.setEndDate(d);
        qm.setErrorCode("error");
        qm.setErrorMessage("errorMessage");
        qm.setHost("host");
        qm.setLastUpdated(d);
        qm.setLastWrittenHash(0);
        qm.setLifecycle(Lifecycle.INITIALIZED);
        qm.setMarkings(markings);
        qm.setNegativeSelectors(negativeSelectors);
        qm.setNumUpdates(0);
        qm.setPageTimes(pageTimes);
        qm.setPositiveSelectors(positiveSelectors);
        qm.setProxyServers(proxyServers);
        qm.setQuery("query");
        qm.setQueryAuthorizations("auths");
        qm.setQueryId("queryId");
        qm.setQueryLogic("queryLogic");
        qm.setQueryType(this.getClass());
        qm.setQueryType("queryType");
        qm.setSetupTime(0);
        qm.setUser("user");
        qm.setUserDN("userDN");
        
        // The version is added to queryMetric objects by default through injection, so we can verify
        // the object on creation.
        assertEquals("3.5.1-TEST", qm.getVersion());
        
        Schema<QueryMetric> schema = (Schema<QueryMetric>) qm.getSchemaInstance();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ProtostuffIOUtil.writeTo(baos, qm, schema, LinkedBuffer.allocate());
        QueryMetric deserializedMetric = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(baos.toByteArray(), deserializedMetric, schema);
        assertEquals(qm, deserializedMetric);
    }
}
