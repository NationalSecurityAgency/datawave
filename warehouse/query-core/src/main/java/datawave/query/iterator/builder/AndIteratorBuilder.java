package datawave.query.iterator.builder;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.filter.field.index.FieldIndexFilterer;
import datawave.query.iterator.logic.AndIterator;

public class AndIteratorBuilder extends AbstractIteratorBuilder {
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> NestedIterator<T> build() {
        if (includes.isEmpty()) {
            throw new IllegalStateException("AndIterator has no inclusive sources!");
        }
        if (fieldIndexFilterNodes != null && !fieldIndexFilterNodes.isEmpty())
            for (NestedIterator include : includes)
                if (include instanceof FieldIndexFilterer)
                    ((FieldIndexFilterer) include).addFieldIndexFilterNodes(fieldIndexFilterNodes);
        return new AndIterator(includes, excludes);
    }
}
