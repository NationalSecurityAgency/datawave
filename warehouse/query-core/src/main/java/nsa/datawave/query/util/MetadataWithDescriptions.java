package nsa.datawave.query.util;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import nsa.datawave.marking.MarkingFunctions;

import nsa.datawave.webservice.results.datadictionary.DescriptionBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.OptionDescriber;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class MetadataWithDescriptions extends Metadata implements Serializable, OptionDescriber {
    
    private static final long serialVersionUID = -3666313025240388292L;
    SetMultimap<MetadataEntry,DescriptionBase> descriptions = HashMultimap.create();
    
    public static final String DESCRIPTIONS = "metadata.descriptions";
    
    public MetadataWithDescriptions() {}
    
    /**
     * @param other
     */
    public MetadataWithDescriptions(MetadataWithDescriptions other) {
        this(Sets.newHashSet(other.datatypes), Sets.newHashSet(other.termFrequencyFields), Sets.newHashSet(other.allFields), Sets
                        .newHashSet(other.indexedFields), Sets.newHashSet(other.indexOnlyFields), HashMultimap.create(other.descriptions));
    }
    
    /**
     * @param helper
     * @throws ExecutionException
     * @throws TableNotFoundException
     */
    public MetadataWithDescriptions(MetadataHelperWithDescriptions helper, Set<String> datatypeFilter) throws ExecutionException, TableNotFoundException,
                    MarkingFunctions.Exception {
        this(Sets.<String> newHashSet(helper.getDatatypes(datatypeFilter)), Sets.<String> newHashSet(helper.getTermFrequencyFields(datatypeFilter)), Sets
                        .<String> newHashSet(helper.getAllFields(datatypeFilter)), Sets.<String> newHashSet(helper.getIndexedFields(datatypeFilter)), Sets
                        .<String> newHashSet(helper.getIndexOnlyFields(datatypeFilter)), HashMultimap.create(helper.getDescriptions(datatypeFilter)));
    }
    
    protected MetadataWithDescriptions(Set<String> datatypes, Set<String> termFrequencyFields, Set<String> allFields, Set<String> indexedFields,
                    Set<String> indexOnlyFields, SetMultimap<MetadataEntry,DescriptionBase> descriptions) {
        this.datatypes = datatypes;
        this.termFrequencyFields = termFrequencyFields;
        this.allFields = allFields;
        this.indexedFields = indexedFields;
        this.indexOnlyFields = indexOnlyFields;
        this.descriptions = descriptions;
    }
}
