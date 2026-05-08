package datawave.query.discovery;

import org.apache.accumulo.core.security.ColumnVisibility;

public interface TermInterface {
    default String getTerm() {
        return "";
    }

    default String getField() {
        return "";
    }

    default String getDate() {
        return "";
    }

    default String getDatatype() {
        return "";
    }

    default ColumnVisibility getVisibility() {
        return new ColumnVisibility();
    }

    default long getUidCount() {
        return 0L;
    }

    default long getUidListSize() {
        return 0L;
    }

    default boolean isValid() {
        return false;
    }
}
