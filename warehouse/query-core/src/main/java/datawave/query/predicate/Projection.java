package datawave.query.predicate;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

import datawave.query.jexl.JexlASTHelper;

/**
 * Predicate that either retains attributes in a specified set of include fields or removes attributes specified in a set of exclude fields.
 * <p>
 * This class is <b>not thread safe</b>
 */
public final class Projection implements Predicate<String> {

    private boolean initialized;

    @Deprecated
    public void setIncludes(Set<String> includes) {
        if (this.initialized) {
            throw new RuntimeException("This Projection instance was already initialized");
        }

        // do not make a copy of the incoming include fields. It could be a UniversalSet
        this.projections = includes;
        this.initialized = true;
        type = ProjectionType.INCLUDES;
    }

    @Deprecated
    public void setExcludes(Set<String> excludes) {
        if (this.initialized) {
            throw new RuntimeException("This Projection instance was already initialized");
        }

        this.projections = Sets.newHashSet(excludes);
        this.initialized = true;
        type = ProjectionType.EXCLUDES;
    }

    @Deprecated
    public Set<String> getIncludes() {
        return Collections.unmodifiableSet(this.projections);
    }

    @Deprecated
    public Set<String> getExcludes() {
        return Collections.unmodifiableSet(this.projections);
    }

    /* Explicit constructor needed now that the new constructor was added */
    @Deprecated
    public Projection() {
        this.initialized = false;
    }

    private Set<String> projections;
    private ProjectionType type;

    public Projection(@Nonnull Set<String> items, @Nonnull ProjectionType type) {
        this.initialized = true;
        this.type = type;
        if (type == ProjectionType.INCLUDES) {
            // do not make a copy of the incoming include fields. It could be a UniversalSet
            this.projections = items;
        } else {
            this.projections = Sets.newHashSet(items);
        }
    }

    public Set<String> getProjections(ProjectionType projectionType) {
        if (this.type == projectionType) {
            return Collections.unmodifiableSet(this.projections);
        } else {
            return Collections.emptySet();
        }
    }

    public boolean isUseIncludes() {
        return type == ProjectionType.INCLUDES;
    }

    public boolean isUseExcludes() {
        return type == ProjectionType.EXCLUDES;
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

        if (type == ProjectionType.EXCLUDES) {
            return !projections.contains(fieldName);
        } else {
            return projections.contains(fieldName);
        }
    }

    public String toString() {
        return new ToStringBuilder(this).append("projections", projections).append("type", type.name()).toString();
    }

    public enum ProjectionType {
        INCLUDES, EXCLUDES;
    }
}
