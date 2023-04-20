package datawave.query.predicate;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Collections;
import java.util.Set;

/**
 * Predicate that either retains attributes in a specified set of include fields or removes attributes specified in a set of exclude fields.
 * <p>
 * This class is <b>not thread safe</b>
 */
public class Projection implements Predicate<String> {
    
    private Set<String> includes = null;
    private Set<String> excludes = null;
    private boolean initialized = false;
    
    public void setIncludes(Set<String> includes) {
        if (this.initialized) {
            throw new RuntimeException("This Projection instance was already initialized");
        }
        
        // do not make a copy of the incoming include fields. It could be a UniversalSet
        this.includes = includes;
        this.initialized = true;
    }
    
    public void setExcludes(Set<String> excludes) {
        if (this.initialized) {
            throw new RuntimeException("This Projection instance was already initialized");
        }
        
        this.excludes = Sets.newHashSet(excludes);
        this.initialized = true;
    }
    
    public Set<String> getIncludes() {
        return Collections.unmodifiableSet(this.includes);
    }
    
    public Set<String> getExcludes() {
        return Collections.unmodifiableSet(this.excludes);
    }
    
    public boolean isUseIncludes() {
        return includes != null;
    }
    
    public boolean isUseExcludes() {
        return excludes != null;
    }
    
    /**
     * Applies this projection to a field name
     *
     * @param inputFieldName
     *            an input field name, possibly with a grouping context or identifier prefix
     * @return true if this field should be kept
     */
    @Override
    public boolean apply(String inputFieldName) {
        if (!this.initialized) {
            throw new RuntimeException("This Projection must be initialized with a set of includes or excludes fields");
        }
        
        String fieldName = JexlASTHelper.deconstructIdentifier(inputFieldName, false);
        
        if (excludes != null) {
            return !excludes.contains(fieldName);
        }
        
        if (includes != null) {
            return includes.contains(fieldName);
        }
        
        throw new RuntimeException("This Projection must be initialized with a set of includes or excludes fields");
    }
    
    public String toString() {
        return new ToStringBuilder(this).append("includes", includes).append("excludes", excludes).toString();
    }
}
