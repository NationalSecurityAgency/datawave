package datawave.query.tables.ssdeep;

import datawave.query.testframework.AbstractFields;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Base field configuration settings for the data in the ssdeep-data CSV file. The default settings are:
 * <ul>
 * <li>forward index: md5, checksum ssdeep</li>
 * <li>indexonly: none</li>
 * <li>reverse: none</li>
 * <li>multivalue fields: none</li>
 * <li>composite index: none</li>
 * <li>virtual fields: none</li>
 * </ul>
 */
public class SSDeepFields extends AbstractFields {

    private static final Collection<String> index = Arrays.asList("CHECKSUM_SSDEEP", SSDeepDataType.SSDeepField.EVENT_ID.name(), SSDeepDataType.SSDeepField.MD5.name());
    private static final Collection<String> indexOnly = new HashSet<>();
    private static final Collection<String> reverse = new HashSet<>();
    private static final Collection<String> multivalue = Arrays.asList();

    private static final Collection<Set<String>> composite = new HashSet<>();
    private static final Collection<Set<String>> virtual = new HashSet<>();

    public SSDeepFields() {
        super(index, indexOnly, reverse, multivalue, composite, virtual);
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" + super.toString() + "}";
    }
}
