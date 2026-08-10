package datawave.marking;

import static datawave.marking.AccessExpressionMarkings.ACCESS;

import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import org.apache.accumulo.access.AccessExpression;

import com.google.common.base.Preconditions;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ColumnVisibilitySecurityMarking implements SecurityMarking {

    public static final String VISIBILITY_MARKING = "columnVisibility";

    @XmlAttribute(name = "columnVisibility")
    private String columnVisibility = null;

    @Override
    public void validate(Map<String,List<String>> parameters) throws IllegalArgumentException {
        List<String> values = parameters.get(VISIBILITY_MARKING);
        if (null == values) {
            throw new IllegalArgumentException("Required parameter " + VISIBILITY_MARKING + " not found");
        }
        if (values.size() != 1) {
            throw new IllegalArgumentException("Required parameter " + VISIBILITY_MARKING + " only accepts one value");
        }
        columnVisibility = values.get(0);
        Preconditions.checkNotNull(columnVisibility);
    }

    public String getColumnVisibility() {
        return toString();
    }

    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    @Override
    public AccessExpression toAccessExpression() {
        if (columnVisibility == null || columnVisibility.isEmpty()) {
            return ACCESS.newExpression("");
        }
        return ACCESS.newExpression(columnVisibility);
    }

    @Override
    public Markings<?> toMarkings() {
        return AccessExpressionMarkings.builder().accessExpression(toAccessExpression()).build();
    }

    @Override
    public int hashCode() {
        return this.columnVisibility == null ? 0 : this.columnVisibility.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof ColumnVisibilitySecurityMarking) {
            ColumnVisibilitySecurityMarking other = (ColumnVisibilitySecurityMarking) obj;
            if (this.columnVisibility != null && other.columnVisibility == null) {
                return false;
            }
            if (this.columnVisibility == null && other.columnVisibility != null) {
                return false;
            }
            if (this.columnVisibility == null) {
                return true;
            }
            return this.columnVisibility.equals(other.columnVisibility);
        }
        return false;
    }

    @Override
    public String toString() {
        return toAccessExpressionString();
    }

    @Override
    public String toAccessExpressionString() {
        if (null == this.columnVisibility) {
            return null;
        }
        return AccessExpressionUtil.normalize(columnVisibility).getExpression();
    }

    public void clear() {
        this.columnVisibility = null;
    }

}
