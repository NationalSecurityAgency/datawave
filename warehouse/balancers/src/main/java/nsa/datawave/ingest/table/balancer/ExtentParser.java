package nsa.datawave.ingest.table.balancer;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.thrift.TKeyExtent;

public interface ExtentParser {
    public String getDate(KeyExtent extent);
    
    public String getDate(TKeyExtent extent);
}
