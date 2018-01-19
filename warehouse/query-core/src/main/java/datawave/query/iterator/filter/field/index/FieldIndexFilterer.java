package datawave.query.iterator.filter.field.index;

import com.google.common.collect.Multimap;
import org.apache.commons.jexl2.parser.JexlNode;

public interface FieldIndexFilterer {
    public void addFieldIndexFilterNodes(Multimap<String,JexlNode> fieldIndexFilterNodes);
}
