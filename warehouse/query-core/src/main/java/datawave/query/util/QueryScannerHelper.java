package datawave.query.util;

import java.util.Collection;

import datawave.query.iterator.QueryInformationIterator;
import datawave.security.util.ScannerHelper;
import datawave.webservice.query.Query;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

/**
 *
 */
public class QueryScannerHelper {

    public static Scanner createScanner(AccumuloClient client, String tableName, Collection<Authorizations> authorizations, Query query)
                    throws TableNotFoundException {
        Scanner scanner = ScannerHelper.createScanner(client, tableName, authorizations);

        scanner.addScanIterator(getQueryInfoIterator(query, false));

        return scanner;
    }

    public static Scanner createScannerWithoutInfo(AccumuloClient client, String tableName, Collection<Authorizations> authorizations, Query query)
                    throws TableNotFoundException {

        return ScannerHelper.createScanner(client, tableName, authorizations);
    }

    public static BatchScanner createBatchScanner(AccumuloClient client, String tableName, Collection<Authorizations> authorizations, int numQueryThreads,
                    Query query) throws TableNotFoundException {
        return createBatchScanner(client, tableName, authorizations, numQueryThreads, query, false);
    }

    public static BatchScanner createBatchScanner(AccumuloClient client, String tableName, Collection<Authorizations> authorizations, int numQueryThreads,
                    Query query, boolean reportErrors) throws TableNotFoundException {
        BatchScanner batchScanner = ScannerHelper.createBatchScanner(client, tableName, authorizations, numQueryThreads);

        batchScanner.addScanIterator(getQueryInfoIterator(query, reportErrors));

        return batchScanner;
    }

    public static IteratorSetting getQueryInfoIterator(Query query, boolean reportErrors) {
        return getQueryInfoIterator(query, reportErrors, null);
    }

    public static IteratorSetting getQueryInfoIterator(Query query, boolean reportErrors, String querystring) {
        QueryInformation info = new QueryInformation(query, querystring);
        IteratorSetting cfg = new IteratorSetting(Integer.MAX_VALUE, QueryInformationIterator.class, info.toMap());
        if (reportErrors)
            QueryInformationIterator.setErrorReporting(cfg);
        return cfg;
    }

    public static IteratorSetting getQueryInfoIterator(Query query) {
        return getQueryInfoIterator(query, false, null);
    }

}
