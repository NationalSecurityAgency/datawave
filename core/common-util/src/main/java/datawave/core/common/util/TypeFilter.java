package datawave.core.common.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

/**
 * Encapsulate filter logic.
 * <ol>
 * <li>null filter means accept everything</li>
 * <li>empty filter means accept everything</li>
 * <li>non-empty filter means restrict to requested things</li>
 * </ol>
 * <p>
 * Note: given a practical example like a datatype filter, a user cannot request zero datatypes. Even if they could, we would never ever want to execute a scan
 * that automatically filters every key it traverses.
 */
public class TypeFilter {

    private static final String EMPTY = "*";

    private final Set<String> elements;

    public TypeFilter() {
        this(null);
    }

    public TypeFilter(Collection<String> elements) {
        this.elements = new HashSet<>();
        if (elements != null) {
            this.elements.addAll(elements);
        }
    }

    public TypeFilter copy() {
        return new TypeFilter(this.elements);
    }

    public void add(String element) {
        this.elements.add(element);
    }

    public void addAll(Collection<String> elements) {
        this.elements.addAll(elements);
    }

    public Set<String> getElements() {
        return this.elements;
    }

    public boolean isEmpty() {
        return this.elements.isEmpty();
    }

    public boolean accept(String candidate) {
        return elements == null || elements.isEmpty() || elements.contains(candidate);
    }

    public boolean contains(String candidate) {
        return elements == null || elements.isEmpty() || elements.contains(candidate);
    }

    public int size() {
        return elements.size();
    }

    @Override
    public String toString() {
        if (elements == null || elements.isEmpty() || (elements.size() == 1 && elements.contains(EMPTY))) {
            return EMPTY;
        }
        return Joiner.on(',').join(elements);
    }

    public static String writeString(Set<String> data) {
        return Joiner.on(',').join(data);
    }

    public static TypeFilter fromString(String data) {
        if (data.startsWith(EMPTY)) {
            return new TypeFilter(null);
        }
        return new TypeFilter(Splitter.on(',').splitToList(data));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TypeFilter filter = (TypeFilter) o;

        return new EqualsBuilder().append(elements, filter.elements).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(elements).toHashCode();
    }
}
