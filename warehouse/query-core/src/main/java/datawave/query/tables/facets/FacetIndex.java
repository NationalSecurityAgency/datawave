package datawave.query.tables.facets;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Lists;

public class FacetIndex {
    protected List<Iterator<Entry<Key,Value>>> children;

    protected String fieldName;
    protected String fieldValue;

    public FacetIndex(String fieldName, String fieldValue) {
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
        children = Lists.newArrayList();
    }

    public FacetIndex() {
        this("", "");
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
    }

    public void addChild(Iterator<Entry<Key,Value>> child) {
        children.add(child);
    }

    public List<Iterator<Entry<Key,Value>>> getChildren() {
        return children;
    }

}
