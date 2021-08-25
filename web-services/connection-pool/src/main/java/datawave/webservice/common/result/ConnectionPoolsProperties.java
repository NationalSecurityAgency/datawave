package datawave.webservice.common.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectionPoolsProperties {
    protected String defaultPool;
    protected Map<String,ConnectionPoolProperties> pools = new HashMap<>();
    
    public String getDefaultPool() {
        return defaultPool;
    }
    
    public Map<String,ConnectionPoolProperties> getPools() {
        return Collections.unmodifiableMap(pools);
    }
    
    public List<String> getPoolNames() {
        return Collections.unmodifiableList(new ArrayList<>(pools.keySet()));
    }
}
