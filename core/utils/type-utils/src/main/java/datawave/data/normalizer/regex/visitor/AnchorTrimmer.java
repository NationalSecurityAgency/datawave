package datawave.data.normalizer.regex.visitor;

import datawave.data.normalizer.regex.EndAnchorNode;
import datawave.data.normalizer.regex.Node;
import datawave.data.normalizer.regex.StartAnchorNode;

/**
 * Implementation of {@link CopyVisitor} that returns a copy of a regex tree trimmed of all start and end anchors to simplify the normalization process.
 */
public class AnchorTrimmer extends CopyVisitor {
    
    public static Node trim(Node node) {
        if (node == null) {
            return null;
        }
        AnchorTrimmer visitor = new AnchorTrimmer();
        return (Node) node.accept(visitor, null);
    }
    
    @Override
    public Object visitStartAnchor(StartAnchorNode node, Object data) {
        return null;
    }
    
    @Override
    public Object visitEndAnchor(EndAnchorNode node, Object data) {
        return null;
    }
}
