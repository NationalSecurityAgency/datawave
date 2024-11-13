package datawave.ingest.table.aggregator;

import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;

import com.google.common.collect.Sets;

public class CombinerConfiguration {

    protected Collection<IteratorSetting.Column> columns;
    protected IteratorSetting setting = null;

    public CombinerConfiguration(IteratorSetting.Column column, IteratorSetting setting) {

        this(Sets.newHashSet(column), setting);
    }

    public CombinerConfiguration(Set<IteratorSetting.Column> columns, IteratorSetting setting) {
        this.columns = columns;
        this.setting = setting;
    }

    public Collection<IteratorSetting.Column> getColumns() {
        return columns;
    }

    public IteratorSetting getSettings() {
        return setting;
    }
}
