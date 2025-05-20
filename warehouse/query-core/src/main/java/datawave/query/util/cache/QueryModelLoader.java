package datawave.query.util.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.query.model.FieldMapping;
import datawave.query.model.ModelKeyParser;
import datawave.query.model.QueryModel;

/**
 *
 */
public class QueryModelLoader extends AccumuloLoader<Entry<String,String>,QueryModel> {

    private static final Logger log = Logger.getLogger(NormalizerLoader.class);

    protected Set<String> allFields = null;

    /**
     * Fetch a query model loader without a known set of fields
     *
     * @param client
     *            a client
     * @param tableName
     *            the table name
     * @param auths
     *            set of auths
     */
    public QueryModelLoader(AccumuloClient client, String tableName, Set<Authorizations> auths) {
        this(client, tableName, auths, null);
    }

    public QueryModelLoader(AccumuloClient client, String tableName, Set<Authorizations> auths, Set<String> allFields) {
        super(client, tableName, auths, new ArrayList<>());

        this.allFields = allFields;

        if (log.isDebugEnabled())
            log.debug("Initializing");

    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.util.cache.AccumuloLoader#buildRange(java.lang.Object)
     */
    @Override
    protected Range buildRange(Entry<String,String> key) {

        return new Range();
    }

    @Override
    protected void build(Entry<String,String> key) throws TableNotFoundException {
        if (null != key) {
            super.build(key, key.getKey());
        }
    }

    @Override
    protected Collection<Text> getColumnFamilies(Entry<String,String> key) {

        Collection<Text> arr = new ArrayList<>();
        arr.add(new Text(key.getValue()));
        return arr;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.cache.CacheLoader#load(java.lang.Object)
     */
    @Override
    public QueryModel load(Entry<String,String> key) throws Exception {

        QueryModel queryModelEntry = entryCache.get(key);
        if (null == queryModelEntry) {
            build(key);

            queryModelEntry = entryCache.get(key);
        }

        return queryModelEntry;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.util.cache.AccumuloLoader#store(java.lang.Object, org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value)
     */
    @Override
    protected boolean store(Entry<String,String> storeKey, Key key, Value value) {
        if (log.isDebugEnabled())
            log.debug("== Requesting to store (" + storeKey.getKey() + "," + storeKey.getValue() + ") " + key);

        // We need the entire Model so we can do both directions.
        QueryModel queryModel = entryCache.get(storeKey);

        if (queryModel == null) {
            queryModel = new QueryModel();
        }

        FieldMapping mapping = ModelKeyParser.parseKey(key, value);
        if (mapping.isFieldMapping()) {
            // Do *not* add a forward mapping entry
            // when the replacement does not exist in the database
            if (allFields == null || allFields.contains(mapping.getFieldName())) {
                switch (mapping.getDirection()) {
                    case FORWARD:
                        queryModel.addTermToModel(mapping.getModelFieldName(), mapping.getFieldName());
                        break;
                    case REVERSE:
                        queryModel.addTermToReverseModel(mapping.getFieldName(), mapping.getModelFieldName());
                        break;
                    default:
                        log.error("Unknown direction: " + mapping.getDirection());
                }
            }
        } else {
            queryModel.setModelFieldAttributes(mapping.getModelFieldName(), mapping.getAttributes());
        }

        entryCache.put(storeKey, queryModel);

        return true;

    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.query.util.cache.Loader#merge(java.lang.Object, java.lang.Object)
     */
    @Override
    protected void merge(Entry<String,String> key, QueryModel value) throws Exception {
        throw new UnsupportedOperationException("Cannot nest in the query model cache");
    }
}
