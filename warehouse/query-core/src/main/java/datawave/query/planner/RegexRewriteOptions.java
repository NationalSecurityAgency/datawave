package datawave.query.planner;

import java.util.Collections;
import java.util.Set;

import datawave.query.jexl.visitors.RegexRewritePattern;
import datawave.query.jexl.visitors.RewriteRegexVisitor;

/**
 * Provides fine-grain control over how the {@link RewriteRegexVisitor} operates pre and post index expansion
 */
public class RegexRewriteOptions {

    private boolean preExpansionEnabled = false;
    private Set<String> preExpansionIncludeFields = Collections.emptySet();
    private Set<String> preExpansionExcludeFields = Collections.emptySet();
    private Set<RegexRewritePattern> preExpansionPatterns = Collections.emptySet();

    private boolean postExpansionEnabled = false;
    private Set<String> postExpansionIncludeFields = Collections.emptySet();
    private Set<String> postExpansionExcludeFields = Collections.emptySet();
    private Set<RegexRewritePattern> postExpansionPatterns = Collections.emptySet();

    public boolean isPreExpansionEnabled() {
        return preExpansionEnabled;
    }

    public void setPreExpansionEnabled(boolean preExpansionEnabled) {
        this.preExpansionEnabled = preExpansionEnabled;
    }

    public Set<String> getPreExpansionIncludeFields() {
        return preExpansionIncludeFields;
    }

    public void setPreExpansionIncludeFields(Set<String> preExpansionIncludeFields) {
        this.preExpansionIncludeFields = preExpansionIncludeFields;
    }

    public Set<String> getPreExpansionExcludeFields() {
        return preExpansionExcludeFields;
    }

    public void setPreExpansionExcludeFields(Set<String> preExpansionExcludeFields) {
        this.preExpansionExcludeFields = preExpansionExcludeFields;
    }

    public Set<RegexRewritePattern> getPreExpansionPatterns() {
        return preExpansionPatterns;
    }

    public void setPreExpansionPatterns(Set<RegexRewritePattern> preExpansionPatterns) {
        this.preExpansionPatterns = preExpansionPatterns;
    }

    public boolean isPostExpansionEnabled() {
        return postExpansionEnabled;
    }

    public void setPostExpansionEnabled(boolean postExpansionEnabled) {
        this.postExpansionEnabled = postExpansionEnabled;
    }

    public Set<String> getPostExpansionIncludeFields() {
        return postExpansionIncludeFields;
    }

    public void setPostExpansionIncludeFields(Set<String> postExpansionIncludeFields) {
        this.postExpansionIncludeFields = postExpansionIncludeFields;
    }

    public Set<String> getPostExpansionExcludeFields() {
        return postExpansionExcludeFields;
    }

    public void setPostExpansionExcludeFields(Set<String> postExpansionExcludeFields) {
        this.postExpansionExcludeFields = postExpansionExcludeFields;
    }

    public Set<RegexRewritePattern> getPostExpansionPatterns() {
        return postExpansionPatterns;
    }

    public void setPostExpansionPatterns(Set<RegexRewritePattern> postExpansionPatterns) {
        this.postExpansionPatterns = postExpansionPatterns;
    }
}
