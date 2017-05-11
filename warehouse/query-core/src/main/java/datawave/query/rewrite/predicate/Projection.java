package datawave.query.rewrite.predicate;

import java.util.Collections;
import java.util.Set;

import datawave.query.rewrite.jexl.JexlASTHelper;

import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

/**
 * Predicate to retain attributes that are specified in a list of fields and remove attributes specified in a blacklist of fields.
 * 
 * This class is <b>not thread safe</b>
 * 
 * 
 * 
 */
public class Projection implements Predicate<String> {
    private static final Logger log = Logger.getLogger(Projection.class);
    private Set<String> whitelist = null, blacklist = null;
    private boolean useWhitelist = false, useBlacklist = false;
    private boolean initialized = false;
    
    public Projection() {}
    
    public void setWhitelist(Set<String> whiteListFields) {
        if (this.initialized) {
            throw new RuntimeException("This Projection instance was already initialized");
        }
        
        this.useWhitelist = true;
        this.whitelist = Sets.newHashSet(whiteListFields);
        this.initialized = true;
    }
    
    public void setBlacklist(Set<String> blackListFields) {
        if (this.initialized) {
            throw new RuntimeException("This Projection instance was already initialized");
        }
        
        this.useBlacklist = true;
        this.blacklist = Sets.newHashSet(blackListFields);
        this.initialized = true;
    }
    
    public Set<String> getWhitelist() {
        return Collections.unmodifiableSet(this.whitelist);
    }
    
    public Set<String> getBlacklist() {
        return Collections.unmodifiableSet(this.blacklist);
    }
    
    public boolean isUseWhitelist() {
        return useWhitelist;
    }
    
    public boolean isUseBlacklist() {
        return useBlacklist;
    }
    
    @Override
    public boolean apply(String inputFieldName) {
        if (!this.initialized) {
            throw new RuntimeException("This Projection must be initialized with a whitelist or blacklist");
        }
        
        String fieldName = JexlASTHelper.deconstructIdentifier(inputFieldName, false);
        
        if (this.useBlacklist) {
            return !this.blacklist.contains(fieldName);
        }
        
        if (this.useWhitelist) {
            return this.whitelist.contains(fieldName);
        }
        
        throw new RuntimeException("This Projection must be initialized with a whitelist or blacklist.");
    }
}
