package datawave.ingest.table.balancer;

import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.TKeyExtent;

public interface ExtentParser {
    public String getDate(KeyExtent extent);
    
    public String getDate(TKeyExtent extent);
}
