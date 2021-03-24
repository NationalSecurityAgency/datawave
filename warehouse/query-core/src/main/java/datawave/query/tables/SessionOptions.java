package datawave.query.tables;

import java.util.Collection;

import datawave.query.config.ShardQueryConfiguration;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;

import com.google.common.collect.Lists;

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
}
