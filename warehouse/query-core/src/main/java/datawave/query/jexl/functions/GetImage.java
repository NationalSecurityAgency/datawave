package datawave.query.jexl.functions;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import com.google.common.base.Function;

class GetImage implements Function<JexlNode,String> {
    public String apply(JexlNode node) {
        return String.valueOf(JexlNodes.getImage(node));
    }
}
