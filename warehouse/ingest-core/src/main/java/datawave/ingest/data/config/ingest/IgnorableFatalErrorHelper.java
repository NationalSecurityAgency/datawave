package datawave.ingest.data.config.ingest;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;

public class IgnorableFatalErrorHelper implements IgnorableErrorHelperInterface {

    /*
     * The list of ignorable fatal error property. Datatype specific values can be specified by prepending the property name with the datatype: e.g.
     * csv.ingest.ignorable.fatal.errors
     */
    public static final String IGNORABLE_FATAL_ERRORS = "ingest.ignorable.fatal.errors";

    private Multimap<Type,String> ignorableFatalErrors = HashMultimap.create();

    public void setup(Configuration conf) {
        if (conf == null) {
            return;
        }

        String[] errors = conf.getStrings(IGNORABLE_FATAL_ERRORS);
        if (errors != null) {
            for (String error : errors) {
                ignorableFatalErrors.put(null, error);
            }
        }

        TypeRegistry registry = TypeRegistry.getInstance(conf);
        // now the datatype specific stuff
        for (Map.Entry<String,String> prop : conf) {
            String propName = prop.getKey();
            if (propName.endsWith(IGNORABLE_FATAL_ERRORS) && !propName.equals(IGNORABLE_FATAL_ERRORS)) {
                String typeName = propName.substring(0, propName.length() - IGNORABLE_FATAL_ERRORS.length() - 1);
                if (registry.containsKey(typeName)) {
                    Type dataType = registry.get(typeName);
                    errors = conf.getStrings(propName);
                    if (errors != null) {
                        for (String error : errors) {
                            ignorableFatalErrors.put(dataType, error);
                        }
                    }
                }
            }
        }
    }

    /**
     * @return true if the error is ignorable
     */
    @Override
    public boolean isIgnorableFatalError(RawRecordContainer e, String err) {
        if (this.ignorableFatalErrors.get(null) != null) {
            if (this.ignorableFatalErrors.get(null).contains(err)) {
                return true;
            }
        }
        if (this.ignorableFatalErrors.get(e.getDataType()) != null) {
            if (this.ignorableFatalErrors.get(e.getDataType()).contains(err)) {
                return true;
            }
        }
        return false;
    }
}
