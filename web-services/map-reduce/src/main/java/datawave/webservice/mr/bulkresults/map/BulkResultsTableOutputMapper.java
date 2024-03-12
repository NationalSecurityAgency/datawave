package datawave.webservice.mr.bulkresults.map;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import datawave.webservice.query.Query;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.exception.EmptyObjectException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.result.BaseQueryResponse;

public class BulkResultsTableOutputMapper extends ApplicationContextAwareMapper<Key,Value,Text,Mutation> {

    public static final String TABLE_NAME = "bulk.results.output.table";
    public static final String QUERY_LOGIC_NAME = "query.logic.name";

    private Text tableName = null;
    private QueryLogicTransformer t = null;
    private Map<Key,Value> entries = new HashMap<>();
    private Map<String,Class<? extends BaseQueryResponse>> responseClassMap = new HashMap<>();
    private SerializationFormat format = SerializationFormat.XML;

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<Key,Value,Text,Mutation>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Query query;
        try {
            String base64EncodedQuery = context.getConfiguration().get(BulkResultsFileOutputMapper.QUERY_LOGIC_SETTINGS);
            Class<? extends Query> queryImplClass = Class.forName(context.getConfiguration().get(BulkResultsFileOutputMapper.QUERY_IMPL_CLASS))
                            .asSubclass(Query.class);
            query = BulkResultsFileOutputMapper.deserializeQuery(base64EncodedQuery, queryImplClass);
        } catch (JAXBException e) {
            throw new RuntimeException("Error deserializing Query", e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Error instantiating query impl class " + context.getConfiguration().get(BulkResultsFileOutputMapper.QUERY_IMPL_CLASS),
                            e);
        }
        QueryLogic<?> logic = (QueryLogic<?>) super.applicationContext.getBean(QUERY_LOGIC_NAME);
        t = logic.getEnrichedTransformer(query);

        this.tableName = new Text(context.getConfiguration().get(TABLE_NAME));
        this.format = SerializationFormat.valueOf(context.getConfiguration().get(BulkResultsFileOutputMapper.RESULT_SERIALIZATION_FORMAT));

    }

    @Override
    protected void map(Key key, Value value, org.apache.hadoop.mapreduce.Mapper<Key,Value,Text,Mutation>.Context context)
                    throws IOException, InterruptedException {
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
                    Value val = BulkResultsFileOutputMapper.serializeResponse(responseClass, response, this.format);
                    // Write out the original key and the new value.
                    Mutation m = new Mutation(key.getRow());
                    m.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), val);
                    context.write(this.tableName, m);
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

}
