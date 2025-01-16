package datawave.core.mapreduce.bulkresults.map;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import datawave.core.query.cache.ResultsPage;
import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.mapreduce.bulkresults.map.SerializationFormat;
import datawave.microservice.query.Query;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.util.ProtostuffMessageBodyWriter;

public class BulkResultsFileOutputMapper extends ApplicationContextAwareMapper<Key,Value,Key,Value> {

    private static Logger log = LoggerFactory.getLogger(BulkResultsFileOutputMapper.class);
    /**
     * Parameter to store Base64 encoded serialized Query settings
     */
    public static final String QUERY_LOGIC_SETTINGS = "query.logic.settings";
    /**
     * Parameter to store the Query impl class name
     */
    public static final String QUERY_IMPL_CLASS = "query.impl.class.name";

    /**
     * Parameter to store class name of the Query Logic
     */
    public static final String QUERY_LOGIC_NAME = "query.logic.name";

    public static final String APPLICATION_CONTEXT_PATH = "application.context.path";

    /**
     * Parameter to store serialization format
     */
    public static final String RESULT_SERIALIZATION_FORMAT = "bulk.results.serial.format";

    private QueryLogicTransformer t = null;
    private Map<Key,Value> entries = new HashMap<>();
    private Map<String,Class<? extends BaseQueryResponse>> responseClassMap = new HashMap<>();
    private SerializationFormat format = SerializationFormat.XML;

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<Key,Value,Key,Value>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Query query;
        try {
            Class<? extends Query> queryImplClass = Class.forName(context.getConfiguration().get(QUERY_IMPL_CLASS)).asSubclass(Query.class);
            query = deserializeQuery(context.getConfiguration().get(QUERY_LOGIC_SETTINGS), queryImplClass);
        } catch (JAXBException e) {
            throw new RuntimeException("Error deserializing Query", e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Error instantiating query impl class " + context.getConfiguration().get(QUERY_IMPL_CLASS), e);
        }
        final Configuration configuration = context.getConfiguration();

        this.setApplicationContext(configuration.get(SPRING_CONFIG_LOCATIONS), configuration.get(SPRING_CONFIG_BASE_PACKAGES),
                        configuration.get(SPRING_CONFIG_STARTING_CLASS));
        String logicName = context.getConfiguration().get(QUERY_LOGIC_NAME);

        QueryLogic<?> logic = (QueryLogic<?>) super.applicationContext.getBean(logicName);
        t = logic.getEnrichedTransformer(query);

        Assert.notNull(logic.getMarkingFunctions());
        Assert.notNull(logic.getResponseObjectFactory());
        this.format = SerializationFormat.valueOf(context.getConfiguration().get(RESULT_SERIALIZATION_FORMAT));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void map(Key key, Value value, org.apache.hadoop.mapreduce.Mapper<Key,Value,Key,Value>.Context context) throws IOException, InterruptedException {
        entries.clear();
        entries.put(key, value);
        for (Entry<Key,Value> entry : entries.entrySet()) {
            try {
                Object o = t.transform(entry);
                BaseQueryResponse response = t.createResponse(new ResultsPage(Collections.singletonList(o)));
                Class<? extends BaseQueryResponse> responseClass = null;
                try {
                    responseClass = getResponseClass(response.getClass().getName());
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Unable to find response class: " + response.getClass().getName(), e);
                }

                try {
                    Value val = serializeResponse(responseClass, response, this.format);
                    // Write out the original key and the new value.
                    if (context.getOutputKeyClass() == null || context.getOutputKeyClass().equals(NullWritable.class)) {
                        // don't write the key in this case, write only the value
                        key = null;
                    } else {
                        key = new Key(key); // to preserve whatever the reason was for this wrapping of the key in the original code
                    }
                    context.write(key, val);
                } catch (Exception e) {
                    throw new RuntimeException("Unable to serialize response of class: " + response.getClass().getName(), e);
                }
                context.progress();
            } catch (EmptyObjectException e) {
                // not yet done, so continue fetching next
            }
        }
    }

    private Class<? extends BaseQueryResponse> getResponseClass(String className) throws ClassNotFoundException {
        if (responseClassMap.containsKey(className))
            return responseClassMap.get(className);
        else {
            @SuppressWarnings("unchecked")
            Class<? extends BaseQueryResponse> clazz = (Class<? extends BaseQueryResponse>) Class.forName(className);
            responseClassMap.put(className, clazz);
            return clazz;
        }
    }

    public static Value serializeResponse(Class<? extends BaseQueryResponse> responseClass, BaseQueryResponse response, SerializationFormat format)
                    throws Exception {
        Value val;
        MediaType media;
        switch (format) {
            case JSON:
                media = MediaType.APPLICATION_JSON_TYPE;
                break;
            case PROTOBUF:
                media = new MediaType("application", "x-protobuf");
                break;
            case YAML:
                media = new MediaType("application", "x-yaml");
                break;
            default:
                media = MediaType.APPLICATION_XML_TYPE;
                break;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        ProtostuffMessageBodyWriter writer = new ProtostuffMessageBodyWriter();

        writer.writeTo(response, responseClass, null, responseClass.getAnnotations(), media, null, baos);

        val = new Value(baos.toByteArray());
        return val;
    }

    public static String serializeQuery(Query q) throws JAXBException {
        StringWriter writer = new StringWriter();
        JAXBContext ctx = JAXBContext.newInstance(q.getClass());
        Marshaller m = ctx.createMarshaller();
        m.marshal(q, writer);
        // Probably need to base64 encode it so that it will not mess up the Hadoop Configuration object
        return new String(Base64.encodeBase64(writer.toString().getBytes()), Charset.forName("UTF-8"));
    }

    public static Query deserializeQuery(String base64EncodedQuery, Class<? extends Query> queryImplClass) throws JAXBException {
        String query = new String(Base64.decodeBase64(base64EncodedQuery), Charset.forName("UTF-8"));
        JAXBContext ctx = JAXBContext.newInstance(queryImplClass);
        Unmarshaller u = ctx.createUnmarshaller();
        return (Query) u.unmarshal(new StringReader(query));
    }

}
