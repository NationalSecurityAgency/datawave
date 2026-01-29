package datawave.query.index.lookup;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.TruncatedIndexScanner;
import datawave.query.util.MetadataHelper;
import datawave.security.util.AuthorizationsMinimizer;

/**
 * A {@link QueryPlanStream} configured to run against a truncated shard index
 */
public class TruncatedRangeStream extends RangeStream {

    private static final Logger log = LoggerFactory.getLogger(TruncatedRangeStream.class);

    public TruncatedRangeStream(ShardQueryConfiguration config, ScannerFactory scanners, MetadataHelper metadataHelper) {
        super(config, scanners, metadataHelper);
    }

    @Override
    public ScannerStream visit(ASTEQNode node, Object data) {

        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        // We are looking for identifier = literal
        JexlASTHelper.IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op == null) {
            return ScannerStream.delayed(node);
        }

        final String fieldName = op.deconstructIdentifier();

        // Null literals cannot be resolved against the index.
        if (op.getLiteralValue() == null) {
            return ScannerStream.delayed(node);
        }

        // toString of String returns the String
        String literal = op.getLiteralValue().toString();

        if (QueryOptions.DEFAULT_DATATYPE_FIELDNAME.equals(fieldName)) {
            return ScannerStream.delayed(node);
        }

        // Check if field is not indexed
        if (!isIndexed(fieldName, config.getIndexedFields())) {
            try {
                if (this.getAllFieldsFromHelper().contains(fieldName)) {
                    if (maxLinesToPrint > 0 && linesPrinted < maxLinesToPrint) {
                        linesPrinted++;
                        log.debug("{'{}':'{}'} is not an observed field.", fieldName, literal);
                    }
                    return ScannerStream.delayed(node);
                }
            } catch (TableNotFoundException e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
            if (maxLinesToPrint > 0 && ++linesPrinted < maxLinesToPrint) {
                linesPrinted++;
                log.debug("{'{}':'{}'} is not indexed.", fieldName, literal);
            }
            // even though the field is not indexed it may still be valuable when evaluating an event. mark this scanner stream as delayed, so it is correctly
            // propagated
            return ScannerStream.delayed(node);
        }

        // Final case, field is indexed
        if (maxLinesToPrint > 0 && linesPrinted < maxLinesToPrint) {
            linesPrinted++;
            log.debug("{'{}':'{}'} is indexed.", fieldName, literal);
        }

        try {
            TruncatedIndexScanner scanner = new TruncatedIndexScanner(config);
            if (config.getDatatypeFilter() != null && !config.getDatatypeFilter().isEmpty()) {
                scanner.setDatatypes(config.getDatatypeFilter());
            }
            scanner.setTableName(config.getTruncatedIndexTableName());
            scanner.setFieldValue(fieldName, literal);
            scanner.setBasePriority(config.getBaseIteratorPriority());

            Authorizations auths = AuthorizationsMinimizer.minimize(config.getAuthorizations()).iterator().next();
            scanner.setAuths(auths);

            return ScannerStream.initialized(scanner, node);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        super.close();
    }
}
