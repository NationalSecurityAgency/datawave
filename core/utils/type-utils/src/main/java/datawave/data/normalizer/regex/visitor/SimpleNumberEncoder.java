package datawave.data.normalizer.regex.visitor;

import datawave.data.normalizer.regex.EncodedNumberNode;
import datawave.data.normalizer.regex.EndAnchorNode;
import datawave.data.normalizer.regex.EscapedSingleCharNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.RegexParser;
import datawave.data.normalizer.regex.RegexUtils;
import datawave.data.normalizer.regex.SingleCharNode;
import datawave.data.normalizer.regex.StartAnchorNode;
import datawave.data.type.util.NumericalEncoder;

/**
 * An implementation of {@link CopyVisitor} that will encode any simple-numbers in the regex pattern, and store them inside a {@link EncodedNumberNode}
 * instance. Any expressions that do not represent a simple number will not be modified. See the following examples:
 * <ul>
 * <li>Input {@code "123\.45"} will return {@code "\+cE1\.2345"}.</li>
 * <li>Input {@code "23.*"} will return {@code "23.*"}.</li>
 * <li>Input {@code "-342|23.*"} will return {@code "!XE6\.58|23.*"}.</li>
 * </ul>
 */
public class SimpleNumberEncoder extends SubExpressionVisitor {
    
    /**
     * Return a copy of the given tree with all simple numbers encoded.
     * 
     * @param node
     *            the node to encode
     * @return the encoded node
     */
    public static Node encode(Node node) {
        if (node == null) {
            return null;
        }
        SimpleNumberEncoder visitor = new SimpleNumberEncoder();
        return (Node) node.accept(visitor, null);
    }
    
    @Override
    protected Object visitSubExpression(Node node) {
        // If the expression is a simple number, encode it.
        if (RegexUtils.isSimpleNumber(node)) {
            Node normalized = normalizeNumber(node);
            return new EncodedNumberNode(normalized.getChildren());
        } else {
            // Otherwise return a copy.
            return copy(node);
        }
    }
    
    /**
     * Create an encoded simple number regex from the given node. It is expected that the given node represents a simple number regex.
     *
     * @param node
     *            the node to encode
     * @return the encoded node.
     */
    private Node normalizeNumber(Node node) {
        // Create a number string from the node. Do not include backlashes or anchor characters.
        StringBuilder sb = new StringBuilder();
        for (Node child : node.getChildren()) {
            if (child instanceof EscapedSingleCharNode) {
                sb.append(((EscapedSingleCharNode) child).getCharacter());
            } else if (child instanceof SingleCharNode) {
                sb.append(((SingleCharNode) child).getCharacter());
            }
        }
        
        // Encode and escape the number.
        String encodedNumber = NumericalEncoder.encode(sb.toString());
        encodedNumber = RegexUtils.escapeEncodedNumber(encodedNumber);
        
        // Parse the number to a node.
        Node encodedNode = RegexParser.parse(encodedNumber);
        
        // If the original expression contained a starting anchor, include it in the encoded node.
        Node firstChild = node.getFirstChild();
        if (firstChild instanceof StartAnchorNode) {
            encodedNode.addChild(firstChild.shallowCopy(), 0);
        }
        
        // If the original expression contained an ending anchor, include it in the encoded node.
        Node lastChild = node.getLastChild();
        if (lastChild instanceof EndAnchorNode) {
            encodedNode.addChild(lastChild.shallowCopy());
        }
        
        return encodedNode;
    }
}
