package nsa.datawave.query.rewrite.jexl.functions;

import org.apache.commons.jexl2.parser.JexlNode;

import com.google.common.base.Function;

class GetImage implements Function<JexlNode,String> {
    public String apply(JexlNode node) {
        return node.image;
    }
}
