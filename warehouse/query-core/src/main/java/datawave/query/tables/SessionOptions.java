package datawave.query.tables;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;

import com.google.common.collect.Lists;

import datawave.query.config.ShardQueryConfiguration;

/**
 * Extension to allow an open constructor
 *
 * Justification: constructor
 */
public class SessionOptions extends ScannerOptions {

    protected ShardQueryConfiguration config;

    public SessionOptions() {
        super();
    }

    public SessionOptions(SessionOptions other) {
        super(other);
        this.config = other.config;
    }

    public void setQueryConfig(ShardQueryConfiguration config) {
        this.config = config;
    }

    public ShardQueryConfiguration getConfiguration() {
        return config;
    }

    public Collection<IteratorSetting> getIterators() {

        Collection<IteratorSetting> settings = Lists.newArrayList();
        for (IterInfo iter : serverSideIteratorList) {
            IteratorSetting setting = new IteratorSetting(iter.getPriority(), iter.getIterName(), iter.getClassName());
            setting.addOptions(serverSideIteratorOptions.get(iter.getIterName()));
            settings.add(setting);
        }
        return settings;
    }

    public void applyExecutionHints(Map<String,String> scanHints) {
        setExecutionHints(scanHints);
    }

    public void applyExecutionHints(String tableName, Map<String,Map<String,String>> tableScanHints) {
        if (tableScanHints.containsKey(tableName)) {
            setExecutionHints(tableScanHints.get(tableName));
        }
    }

    public void applyConsistencyLevel(ConsistencyLevel consistencyLevel) {
        setConsistencyLevel(consistencyLevel);
    }

    public void applyConsistencyLevel(String tableName, Map<String,ConsistencyLevel> consistencyLevels) {
        if (consistencyLevels.containsKey(tableName)) {
            setConsistencyLevel(consistencyLevels.get(tableName));
        }
    }
}
