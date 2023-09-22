package datawave.ingest.table.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;

public class DateIndexTableConfigHelper extends AbstractTableConfigHelper {

    protected Logger log;

    protected Configuration conf;
    protected String tableName;
    public static final String LOCALITY_GROUPS = "date.index.table.locality.groups";
    HashMap<String,Set<Text>> localityGroups = new HashMap<>();

    @Override
    public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {

        this.log = log;
        this.conf = config;
        this.tableName = tableName;

        if (this.tableName == null) {
            throw new IllegalArgumentException("No DateIndex Table Defined");
        }

        String localityGroupsConf = null;
        localityGroupsConf = conf.get(LOCALITY_GROUPS,
                        ExtendedDataTypeHandler.FULL_CONTENT_LOCALITY_NAME + ':' + ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY + ','
                                        + ExtendedDataTypeHandler.TERM_FREQUENCY_LOCALITY_NAME + ':' + ExtendedDataTypeHandler.TERM_FREQUENCY_COLUMN_FAMILY);
        for (String localityGroupDefConf : StringUtils.split(localityGroupsConf)) {
            String[] localityGroupDef = StringUtils.split(localityGroupDefConf, '\\', ':');
            Set<Text> families = localityGroups.get(localityGroupDef[0]);
            if (families == null) {
                families = new HashSet<>();
                localityGroups.put(localityGroupDef[0], families);
            }
            families.add(new Text(localityGroupDef[1]));
        }
    }

    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        configureDateIndexTable(tops);
    }

    protected void configureDateIndexTable(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        // Add the DATE aggregator
        for (IteratorScope scope : IteratorScope.values()) {
            String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "DATEAggregator");
            setPropertyIfNecessary(tableName, stem, "19,datawave.iterators.TotalAggregatingIterator", tops, log);
            stem += ".opt.";
            setPropertyIfNecessary(tableName, stem + "*", "datawave.ingest.table.aggregator.DateIndexDateAggregator", tops, log);
        }

        setPropertyIfNecessary(tableName, Property.TABLE_BLOOM_ENABLED.getKey(), Boolean.toString(false), tops, log);

        // Set the locality group for the full content column family
        setLocalityGroupConfigurationIfNecessary(tableName, localityGroups, tops, log);
    }

}
