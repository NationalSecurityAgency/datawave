package datawave.ingest.table.config;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.summary.MetricsSummaryDataTypeHandler;
import datawave.ingest.table.bloomfilter.ShardIndexKeyFunctor;

public class MetricsSummaryTableConfigHelper extends AbstractTableConfigHelper {
    protected Logger mLog;

    protected Configuration mConf;
    protected String mTableName;

    public static final String ENABLE_BLOOM_FILTERS = MetricsSummaryDataTypeHandler.METRICS_SUMMARY_PROP_PREFIX + "summary.enable.bloom.filters";
    protected boolean mEnableBloomFilters = false;

    @Override
    public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {
        mLog = log;
        mConf = config;
        mTableName = tableName;

        String mMetricsSummaryTableName = mConf.get(MetricsSummaryDataTypeHandler.METRICS_SUMMARY_TABLE_NAME);

        if (mMetricsSummaryTableName == null) {
            throw new IllegalArgumentException("Metrics Summary Table is not defined in the Configuration.");
        }

        mEnableBloomFilters = mConf.getBoolean(ENABLE_BLOOM_FILTERS, false);

        if (!mTableName.equals(mMetricsSummaryTableName)) {
            throw new IllegalArgumentException("Invalid Metrics Summary Table Definition For: " + mTableName);
        }
    }

    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        // add the SummingCombiner Iterator for each iterator scope (majc, minc, scan)
        for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
            final StringBuilder propName = new StringBuilder(String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "sum"));
            setPropertyIfNecessary(mTableName, propName.toString(), "19,org.apache.accumulo.core.iterators.user.SummingCombiner", tops, mLog);
            propName.append(".opt.");
            setPropertyIfNecessary(mTableName, propName + "all", "true", tops, mLog);
            setPropertyIfNecessary(mTableName, propName + "lossy", "FALSE", tops, mLog);
            setPropertyIfNecessary(mTableName, propName + "type", "STRING", tops, mLog);
        }

        // enable bloom filters if necessary.
        if (mEnableBloomFilters) {
            setPropertyIfNecessary(mTableName, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), ShardIndexKeyFunctor.class.getName(), tops, mLog);
        }
        setPropertyIfNecessary(mTableName, Property.TABLE_BLOOM_ENABLED.getKey(), Boolean.toString(mEnableBloomFilters), tops, mLog);
    }
}
