package nsa.datawave.query.tables;

import java.util.Collection;

import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.data.thrift.IterInfo;

import com.google.common.collect.Lists;

/**
 * Extension to allow an open constructor
 * 
 * Justification: constructor
 */
public class SessionOptions extends ScannerOptions {
    
    protected RefactoredShardQueryConfiguration config;
    
    public SessionOptions() {
        super();
    }
    
    public SessionOptions(SessionOptions other) {
        super(other);
        this.config = other.config;
    }
    
    public void setQueryConfig(RefactoredShardQueryConfiguration config) {
        this.config = config;
    }
    
    public RefactoredShardQueryConfiguration getConfiguration() {
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
