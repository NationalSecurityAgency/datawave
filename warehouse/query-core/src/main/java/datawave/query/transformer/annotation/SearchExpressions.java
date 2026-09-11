package datawave.query.transformer.annotation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable, de-duplicated collection of structured search expressions. */
public final class SearchExpressions implements Iterable<SearchExpression>, Serializable {
    private static final long serialVersionUID = 1L;
    private final List<SearchExpression> expressions;

    public SearchExpressions() {
        this(Collections.emptyList());
    }

    public SearchExpressions(Collection<? extends SearchExpression> expressions) {
        Objects.requireNonNull(expressions, "expressions");
        Set<SearchExpression> unique = new LinkedHashSet<>(expressions);
        this.expressions = Collections.unmodifiableList(new ArrayList<>(unique));
    }

    public List<SearchExpression> getExpressions() {
        return expressions;
    }

    public int size() {
        return expressions.size();
    }

    public boolean isEmpty() {
        return expressions.isEmpty();
    }

    @Override
    public Iterator<SearchExpression> iterator() {
        return expressions.iterator();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SearchExpressions && new LinkedHashSet<>(expressions).equals(new LinkedHashSet<>(((SearchExpressions) other).expressions));
    }

    @Override
    public int hashCode() {
        return new LinkedHashSet<>(expressions).hashCode();
    }
}
