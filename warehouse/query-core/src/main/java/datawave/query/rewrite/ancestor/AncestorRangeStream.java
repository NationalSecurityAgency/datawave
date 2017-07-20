package datawave.query.rewrite.ancestor;

import datawave.query.index.lookup.AncestorCondensedUidIterator;
import datawave.query.index.lookup.AncestorIndexStream;
import datawave.query.index.lookup.IndexStream;
import datawave.query.index.lookup.RangeStream;
import datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTOrNode;

/**
 * Prevent ranges that are from the same document from both being returned, resulting in duplicate rows across the ancestor
 */
public class AncestorRangeStream extends RangeStream {
    public AncestorRangeStream(RefactoredShardQueryConfiguration config, ScannerFactory scanners, MetadataHelper metadataHelper) {
        super(config, scanners, metadataHelper);
        setCreateCondensedUidIteratorClass(AncestorCondensedUidIterator.class);
    }
    
    @Override
    public IndexStream visit(ASTOrNode node, Object data) {
        IndexStream unmerged = super.visit(node, data);
        
        return new AncestorIndexStream(unmerged, node);
    }
    
    @Override
    public IndexStream visit(ASTAndNode node, Object data) {
        IndexStream unmerged = super.visit(node, data);
        
        return new AncestorIndexStream(unmerged, node);
    }
}
