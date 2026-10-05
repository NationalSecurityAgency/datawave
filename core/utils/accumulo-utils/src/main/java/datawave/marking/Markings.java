package datawave.marking;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Markings<T> {

    T getMarkings();

    AccessExpression toAccessExpression();

    ColumnVisibility toColumnVisibility();

    default boolean isEmpty() {
        return getMarkings() == null;
    }
}
