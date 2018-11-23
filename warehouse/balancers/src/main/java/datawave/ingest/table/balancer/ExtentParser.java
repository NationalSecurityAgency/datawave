package datawave.ingest.table.balancer;

import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.TKeyExtent;

public interface ExtentParser {
    String getDate(KeyExtent extent);
    
    String getDate(TKeyExtent extent);
}
