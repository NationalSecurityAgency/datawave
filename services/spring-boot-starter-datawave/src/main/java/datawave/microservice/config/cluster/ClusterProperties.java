package datawave.microservice.config.cluster;

import datawave.microservice.config.accumulo.AccumuloProperties;

/**
 * Base class for cluster-specific properties that many microservices will need, i.e., properties that are common to both "metrics" and "warehouse" clusters
 */
public abstract class ClusterProperties {
    
    private AccumuloProperties accumulo;
    
    public AccumuloProperties getAccumulo() {
        return accumulo;
    }
    
    public void setAccumulo(AccumuloProperties accumulo) {
        this.accumulo = accumulo;
    }
}
