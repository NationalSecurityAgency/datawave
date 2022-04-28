package datawave.query.tables.document.batch;

import datawave.query.DocumentSerialization;
import datawave.query.iterator.QueryInformationIterator;
import datawave.query.util.QueryInformation;
import datawave.security.iterator.ConfigurableVisibilityFilter;
import datawave.security.util.AuthorizationsMinimizer;
import datawave.webservice.common.connection.ScannerBaseDelegate;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.Iterator;

public class DocumentScannerHelper {

    public static DocumentScannerImpl createDocumentBatchScanner(AccumuloClient client, String tableName, Collection<Authorizations> authorizations, int numQueryThreads, Query query, boolean docRawFields, DocumentSerialization.ReturnType returnType, int queueCapacity, int maxTabletsPerRequest, int maxTabletThreshold) throws TableNotFoundException {
        DocumentScannerImpl batchScanner = null;
        if (authorizations != null && !authorizations.isEmpty()) {
            Iterator<Authorizations> iter = AuthorizationsMinimizer.minimize(authorizations).iterator();
            batchScanner = new DocumentScannerImpl((ClientContext)client, TableId.of((String)((ClientContext)client).tableOperations().tableIdMap().get(tableName)), tableName, (Authorizations)iter.next(), numQueryThreads, returnType, docRawFields, queueCapacity, maxTabletsPerRequest, maxTabletThreshold);
            addVisibilityFilters(iter, batchScanner);
            if (null != query)
                batchScanner.addScanIterator(getQueryInfoIterator(query, false));
            return batchScanner;
        } else {
            throw new IllegalArgumentException("Authorizations must not be empty.");
        }
    }


    public static IteratorSetting getQueryInfoIterator(Query query, boolean reportErrors) {
        return getQueryInfoIterator(query, reportErrors, (String)null);
    }

    public static IteratorSetting getQueryInfoIterator(Query query, boolean reportErrors, String querystring) {
        QueryInformation info = new QueryInformation(query, querystring);
        IteratorSetting cfg = new IteratorSetting(2147483647, QueryInformationIterator.class, info.toMap());
        if (reportErrors) {
            QueryInformationIterator.setErrorReporting(cfg);
        }

        return cfg;
    }

    public static IteratorSetting getQueryInfoIterator(Query query) {
        return getQueryInfoIterator(query, false, (String)null);
    }

    protected static void addVisibilityFilters(Iterator<Authorizations> iter, ScannerBase scanner) {
        for(int priority = 10; iter.hasNext(); ++priority) {
            IteratorSetting cfg = new IteratorSetting(priority, ConfigurableVisibilityFilter.class);
            cfg.setName("visibilityFilter" + priority);
            cfg.addOption("authorizations", ((Authorizations)iter.next()).toString());
            if (scanner instanceof ScannerBaseDelegate) {
                ((ScannerBaseDelegate)scanner).addSystemScanIterator(cfg);
            } else {
                scanner.addScanIterator(cfg);
            }
        }

    }
}
