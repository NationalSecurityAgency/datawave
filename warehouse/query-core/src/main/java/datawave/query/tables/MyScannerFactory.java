package datawave.query.tables;

import datawave.query.DocumentSerialization;
import datawave.query.config.DocumentQueryConfiguration;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.document.batch.DocumentScannerBase;
import datawave.query.tables.document.batch.DocumentScannerHelper;
import datawave.query.tables.document.batch.DocumentScannerImpl;
import datawave.webservice.common.connection.WrappedConnector;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.util.Set;

public class MyScannerFactory extends ScannerFactory{
    private static final Logger log = Logger.getLogger(MyScannerFactory.class);

    public MyScannerFactory(GenericQueryConfiguration queryConfiguration) {
        super(queryConfiguration);
        this.cxn = queryConfiguration.getClient();

        if (queryConfiguration instanceof DocumentQueryConfiguration) {
            this.settings = ((ShardQueryConfiguration) queryConfiguration).getQuery();
            this.accrueStats = ((ShardQueryConfiguration) queryConfiguration).getAccrueStats();
        }
        log.debug("Created scanner factory " + System.identityHashCode(this) + " is wrapped ? " + (cxn instanceof WrappedConnector));

        if (queryConfiguration instanceof DocumentQueryConfiguration) {
            config = ((ShardQueryConfiguration) queryConfiguration);
            maxQueue = ((ShardQueryConfiguration) queryConfiguration).getMaxScannerBatchSize();
            this.settings = ((ShardQueryConfiguration) queryConfiguration).getQuery();
            try {
                scanQueue = new ResourceQueue(((ShardQueryConfiguration) queryConfiguration).getNumQueryThreads(), this.cxn, new AccumuloResource.AccumuloResourceFactory(this.cxn));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized DocumentScannerBase newDocumentScanner(DocumentQueryConfiguration config) throws TableNotFoundException {
        return newDocumentScanner(config.getShardTableName(), config.getAuthorizations(), config.getNumQueryThreads(),
                config.getQuery(),config.getDocRawFields(), config.getReturnType(), config.getQueueCapacity() == 0 ? config.getNumQueryThreads() : config.getQueueCapacity(), config.getMaxTabletsPerRequest(), config.getMaxTabletThreshold());
    }


    public synchronized DocumentScannerBase newDocumentScanner(String tableName, Set<Authorizations> auths, int threads, Query query, boolean docRawFields, DocumentSerialization.ReturnType returnType, int queueCapacity, int maxTabletsPerRequest, int maxTabletThreshold) throws TableNotFoundException {
            DocumentScannerBase bs = DocumentScannerHelper.createDocumentBatchScanner(this.cxn, tableName, auths, threads, query, docRawFields, returnType, queueCapacity, maxTabletsPerRequest, maxTabletThreshold);
            log.debug("Created scanner " + System.identityHashCode(bs));
            if (log.isTraceEnabled()) {
                log.trace("Adding instance " + bs.hashCode());
            }

            this.instances.add(bs);
            return bs;
    }

    public synchronized boolean close(ScannerBase bs) {
        boolean removed = instances.remove(bs);
        if (removed) {
            log.debug("Closed scanner " + System.identityHashCode(bs));
            if (log.isTraceEnabled()) {
                log.trace("Closing instance " + bs.hashCode());
            }
            bs.close();
        }
        return removed;
    }

}
