package datawave.query.util.cache;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.data.ColumnFamilyConstants;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.Constants;

/**
 *
 */
public class NormalizerLoader extends AccumuloLoader<String,Multimap<String,Type<?>>> {

    protected Collection<String> dataTypeFilters;

    private static final Logger log = Logger.getLogger(NormalizerLoader.class);

    public NormalizerLoader(AccumuloClient client, String tableName, Set<Authorizations> auths, Collection<Text> columnFamilyList,
                    Collection<String> dataTypeFilters) {
        super(client, tableName, auths, columnFamilyList);

        if (null != dataTypeFilters)
            this.dataTypeFilters = new ArrayList<>(dataTypeFilters);
        else
            this.dataTypeFilters = new ArrayList<>();

        if (log.isTraceEnabled())
            log.trace("Initializing");

    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.util.cache.AccumuloLoader#buildRange(java.lang.Object)
     */
    @Override
    protected Range buildRange(String key) {
        Preconditions.checkNotNull(key);

        if (log.isTraceEnabled())
            log.trace("Building range on " + key);

        return new Range(key.toUpperCase());
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.cache.CacheLoader#load(java.lang.Object)
     */
    @Override
    public Multimap<String,Type<?>> load(String key) throws Exception {

        if (entryCache.isEmpty()) {
            super.load(null);
        }

        synchronized (entryCache) {
            if (log.isTraceEnabled())
                log.trace("Getting key " + key);
            return entryCache.get(key);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.util.cache.AccumuloLoader#store(java.lang.Object, org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value)
     */
    @Override
    protected boolean store(String storeKey, Key key, Value value) {
        boolean reverse = false;

        if (log.isTraceEnabled())
            log.trace("== Requesting to store " + storeKey + " " + key);

        if (null != key.getColumnQualifier()) {
            String colq = key.getColumnQualifier().toString();
            int idx = colq.indexOf(Constants.NULL);

            if (key.getColumnFamily().toString().equals(ColumnFamilyConstants.COLF_RI.toString())) {
                reverse = true;
            }

            if (idx != -1) {
                try {
                    String type = colq.substring(0, idx);

                    Multimap<String,Type<?>> dataNormalizer = entryCache.get("DATA_TYPES");

                    if (null == dataNormalizer) {
                        dataNormalizer = ArrayListMultimap.create();
                        entryCache.put("DATA_TYPES", dataNormalizer);
                    }

                    dataNormalizer.put(type, LcNoDiacriticsType.class.getDeclaredConstructor().newInstance());

                    if (!dataTypeFilters.isEmpty() && !dataTypeFilters.contains(type)) {
                        if (log.isTraceEnabled())
                            log.trace("Returning since " + type + " is not in the data type cache");
                        return false;
                    }

                    String row = key.getRow().toString().toUpperCase();

                    if (reverse)
                        row = new StringBuffer(row).reverse().toString();

                    Multimap<String,Type<?>> typedNormalizers = entryCache.get(row);

                    if (typedNormalizers == null) {
                        typedNormalizers = ArrayListMultimap.create();
                        entryCache.put(row, typedNormalizers);
                    }

                    @SuppressWarnings("unchecked")
                    Class<? extends Type<?>> clazz = (Class<? extends Type<?>>) Class.forName(colq.substring(idx + 1));

                    Type<?> normalizer = clazz.newInstance();

                    typedNormalizers.put(type, normalizer);

                    typedNormalizers = entryCache.get(type);

                    if (typedNormalizers == null) {
                        typedNormalizers = ArrayListMultimap.create();
                        entryCache.put(type, typedNormalizers);
                    }

                    typedNormalizers.put(row, normalizer);

                    typedNormalizers = entryCache.get(clazz.getCanonicalName());

                    if (typedNormalizers == null) {
                        typedNormalizers = ArrayListMultimap.create();
                        entryCache.put(clazz.getCanonicalName(), typedNormalizers);
                    }

                    typedNormalizers.put(row, normalizer);

                    return true;

                } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                    log.error("Unable to find normalizer on class path: " + colq.substring(idx + 1), e);
                }

            }
        }
        return false;

    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.query.util.cache.Loader#merge(java.lang.Object, java.lang.Object)
     */
    @Override
    protected void merge(String key, Multimap<String,Type<?>> value) throws Exception {

        entryCache.get(key).putAll(value);

    }
}
