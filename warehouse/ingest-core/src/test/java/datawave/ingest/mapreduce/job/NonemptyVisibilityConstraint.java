package datawave.ingest.mapreduce.job;

import com.google.common.collect.Multimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * Verifies that a mutation has a non-null, non-empty visibility. For testing.
 */
public class NonemptyVisibilityConstraint implements VisibilityConstraint {
    @Override
    public boolean isValid(byte[] visibility) {
        return visibility != null && visibility.length > 0;
    }
    
    /**
     * Initializer to setup this constraint.
     */
    public static class Initializer implements ConstraintInitializer {
        
        // since it's just a test, only allow for configuring on one table
        public static String TABLE_CONFIG = "nonempty.visibility.table";
        
        @Override
        public void addConstraints(Configuration conf, Multimap<Text,VisibilityConstraint> constraintsMap) {
            String table = conf.get(TABLE_CONFIG);
            
            if (table == null || table.trim().isEmpty()) {
                throw new IllegalArgumentException(this.getClass() + " requires the config: " + TABLE_CONFIG);
            }
            
            constraintsMap.put(new Text(table), new NonemptyVisibilityConstraint());
        }
    }
}
