package datawave.microservice;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import lombok.Data;

@Data
public class Connection {
    
    private AccumuloClient accumuloClient;
    private Set<Authorizations> auths;
    private String metadataTable;
    private String modelTable;
    private String modelName;
}
