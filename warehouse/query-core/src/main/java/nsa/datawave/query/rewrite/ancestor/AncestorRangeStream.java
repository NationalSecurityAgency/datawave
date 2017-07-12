package nsa.datawave.query.rewrite.ancestor;

import nsa.datawave.query.index.lookup.AncestorCondensedUidIterator;
import nsa.datawave.query.index.lookup.AncestorIndexStream;
import nsa.datawave.query.index.lookup.IndexStream;
import nsa.datawave.query.index.lookup.RangeStream;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.query.util.MetadataHelper;
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
