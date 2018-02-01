package datawave.ingest.mapreduce.handler.edge.define;

import datawave.ingest.mapreduce.handler.edge.define.EdgeDirection;

/**
 * Edge Direction enumeration
 * 
 * 
 *
 */
public enum EdgeDirection {
    UNIDIRECTIONAL("uni"), BIDIRECTIONAL("bi");
    
    public final String confLabel;
    
    EdgeDirection(String confLabel) {
        this.confLabel = confLabel;
    }
    
    public static EdgeDirection parse(String token) {
        for (EdgeDirection direction : EdgeDirection.values()) {
            if (direction.confLabel.equals(token)) {
                return direction;
            }
        }
        return null; // default is unidirectional
    }
}
