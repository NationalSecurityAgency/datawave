package datawave.ingest.mapreduce.job;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Runs all configured VisibilityConstraints for all tables.
 */
public class ConstraintChecker {

    private static final Logger log = Logger.getLogger(ConstraintChecker.class);

    public static final String INITIALIZERS = "visibility.constraint.initializers";

    /**
     * Factory method to create a new ConstraintChecker from a Configuration.
     *
     * @param conf
     *            a configuration
     * @return constraint checker
     */
    public static ConstraintChecker create(Configuration conf) {

        Multimap<Text,VisibilityConstraint> constraints = null;

        String[] initializerClasses = conf.getStrings(INITIALIZERS);

        if (initializerClasses != null) {
            for (String initializerClass : initializerClasses) {
                if (constraints == null) {
                    constraints = HashMultimap.create();
                }

                try {
                    ConstraintInitializer initializer = Class.forName(initializerClass).asSubclass(ConstraintInitializer.class).newInstance();
                    initializer.addConstraints(conf, constraints);

                } catch (Exception e) {
                    log.error("Could invoke ConstraintInitializer: " + initializerClass, e);
                    throw new RuntimeException("Could invoke ConstraintInitializer: " + initializerClass, e);
                }
            }
        }

        return new ConstraintChecker(constraints);
    }

    private final Multimap<Text,VisibilityConstraint> constraints;
    private final boolean isConfigured;

    private ConstraintChecker(Multimap<Text,VisibilityConstraint> constraints) {
        this.constraints = constraints;
        this.isConfigured = (constraints != null && !constraints.isEmpty());
    }

    /**
     * Tells if this feature is currently enabled.
     *
     * @return true if constraint checking has been setup, false, otherwise.
     */
    public boolean isConfigured() {
        return isConfigured;
    }

    /**
     * Runs the configured validation on the given table/visibility.
     *
     * @param table
     *            the table
     * @param visibility
     *            the visibility
     * @throws ConstraintViolationException
     *             If the constraints are not satisfied
     */
    public void check(Text table, byte[] visibility) throws ConstraintViolationException {
        // fast return if we are not configured
        if (!isConfigured) {
            return;
        }

        Collection<VisibilityConstraint> tableConstraints = constraints.get(table);

        if (tableConstraints != null && !tableConstraints.isEmpty()) {
            for (VisibilityConstraint constraint : tableConstraints) {
                if (!constraint.isValid(visibility)) {
                    throw new ConstraintViolationException(table, constraint);
                }
            }
        }
    }

    /**
     * Thrown when a violation is encountered.
     */
    public static class ConstraintViolationException extends RuntimeException {
        public ConstraintViolationException(Text table, VisibilityConstraint constraint) {
            super(String.format("Visibility constraint '%s' violated for table '%s'", constraint, table));
        }
    }
}
