package datawave.query.testframework;

import datawave.query.tables.ssdeep.SSDeepDataType;

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
public class GenericSSDeepFields extends AbstractFields {

    private static final Collection<String> index = Arrays.asList(SSDeepDataType.SSDeepField.MD5.name(), SSDeepDataType.SSDeepField.CHECKSUM_SSDEEP.name());
    private static final Collection<String> indexOnly = new HashSet<>();
    private static final Collection<String> reverse = new HashSet<>();
    private static final Collection<String> multivalue = Arrays.asList();

    private static final Collection<Set<String>> composite = new HashSet<>();
    private static final Collection<Set<String>> virtual = new HashSet<>();

    public GenericSSDeepFields() {
        super(index, indexOnly, reverse, multivalue, composite, virtual);
    }
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" + super.toString() + "}";
    }
}
