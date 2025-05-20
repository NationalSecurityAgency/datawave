package datawave.ingest.data.config;

import org.apache.hadoop.conf.Configuration;

public interface MaskedFieldHelper {

    /**
     * Configure this helper before it's first use
     *
     * @param config
     *            Hadoop configuration object
     */
    void setup(Configuration config);

    /**
     * @return true if there exists any mappings
     */
    boolean hasMappings();

    /**
     * @param key
     *            field name for which to retrieve the mapping
     * @return true if there exists a mapping for this key else false
     */
    boolean contains(final String key);

    /**
     * @param key
     *            field name for which to retrieve the mapping
     * @return moved to meth
     */
    String get(final String key);

}
