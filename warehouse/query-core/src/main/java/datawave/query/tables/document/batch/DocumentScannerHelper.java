package datawave.query.tables.document.batch;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.query.DocumentSerialization;
import datawave.query.iterator.QueryInformationIterator;
import datawave.query.util.QueryInformation;
import datawave.query.util.QueryScannerHelper;
import datawave.security.iterator.ConfigurableVisibilityFilter;
import datawave.security.util.AuthorizationsMinimizer;
import datawave.webservice.common.connection.ScannerBaseDelegate;
import datawave.webservice.common.connection.WrappedAccumuloClient;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.Iterator;

public class DocumentScannerHelper {

    public static DocumentScannerBase createDocumentBatchScanner(AccumuloClient client, String tableName, Collection<Authorizations> authorizations, int numQueryThreads, Query query, boolean docRawFields, DocumentSerialization.ReturnType returnType, int queueCapacity, int maxTabletsPerRequest, int maxTabletThreshold) throws TableNotFoundException {
        DocumentScannerBase batchScanner = null;
        if (authorizations != null && !authorizations.isEmpty()) {
            Iterator<Authorizations> iter = AuthorizationsMinimizer.minimize(authorizations).iterator();
            try {
                batchScanner = getScanner(client,tableName,iter,numQueryThreads,query,docRawFields,returnType,queueCapacity, maxTabletsPerRequest,maxTabletThreshold);
            } catch (AccumuloException e) {
                throw new RuntimeException(e);
            } catch (AccumuloSecurityException e) {
                throw new RuntimeException(e);
            }
            QueryScannerHelper.addVisibilityFilters(iter, batchScanner);
            if (null != query)
                batchScanner.addScanIterator(QueryScannerHelper.getQueryInfoIterator(query, false));
            return batchScanner;
        } else {
            throw new IllegalArgumentException("Authorizations must not be empty.");
        }
    }

    public static DocumentScannerBase getScanner(AccumuloClient client, String tableName, Iterator<Authorizations> iter, int numQueryThreads, Query query, boolean docRawFields, DocumentSerialization.ReturnType returnType, int queueCapacity, int maxTabletsPerRequest, int maxTabletThreshold) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        ClientContext ctx = getClientContext(client);
        if (ctx instanceof InMemoryAccumuloClient){
            return new InMemoryDocumentScannerImpl(ctx, TableId.of((String) ctx.tableOperations().tableIdMap().get(tableName)), tableName, (Authorizations) iter.next(), numQueryThreads, returnType, docRawFields);
        }
        else {
            return new DocumentScannerImpl(ctx, TableId.of((String) ctx.tableOperations().tableIdMap().get(tableName)), tableName, (Authorizations) iter.next(), numQueryThreads, returnType, docRawFields, queueCapacity, maxTabletsPerRequest, maxTabletThreshold);
        }
    }




    protected static ClientContext getClientContext(AccumuloClient client) {
        if (client instanceof WrappedAccumuloClient) {
            return (ClientContext)((WrappedAccumuloClient) client).getReal();
        }
        return (ClientContext) client;
    }
}
