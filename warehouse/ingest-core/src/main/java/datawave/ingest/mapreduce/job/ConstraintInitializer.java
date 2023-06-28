package datawave.ingest.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Multimap;

/**
 * A component which can place additional constraint checking on ingest.
 */
public interface ConstraintInitializer {

    /**
     * Creates and adds constraints to the given constraint map (map of table name to constraints).
     *
     * @param conf
     *            a configuration
     * @param constraintsMap
     *            the constraints map
     */
    void addConstraints(Configuration conf, Multimap<Text,VisibilityConstraint> constraintsMap);
}
