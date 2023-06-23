package datawave.query.tables.term;

import com.google.common.collect.ImmutableSet;
import datawave.core.common.connection.AccumuloConnectionFactory.Priority;
import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.query.QueryParameters;
import datawave.query.config.TermFrequencyQueryConfiguration;
import datawave.query.transformer.TermFrequencyQueryTransformer;
import datawave.query.util.QueryScannerHelper;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;

import static datawave.query.Constants.NULL;

public class TermFrequencyQueryTable extends BaseQueryLogic<Entry<Key,Value>> {

    protected static final Logger log = ThreadConfigurableLogger.getLogger(TermFrequencyQueryTable.class);

    private static final String PARENT_ONLY = "\1";
    private static final String ALL = "\u10FFFF";

    public TermFrequencyQueryTable() {
        super();
        log.debug("Creating TermFrequencyQueryTable: " + System.identityHashCode(this));
    }

    public TermFrequencyQueryTable(BaseQueryLogic<Entry<Key,Value>> other) {
        super(other);
        log.debug("Creating TermFrequencyQueryTable: " + System.identityHashCode(this));
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new TermFrequencyQueryTable(this);
    }

    @Override
    public Priority getConnectionPriority() {
        return Priority.NORMAL;
    }

    @Override
    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        return ImmutableSet.of("termfrequency.field.name", "termfrequency.match.term", "termfrequency.find.term", QueryParameters.CONTENT_VIEW_ALL);
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        return Collections.emptySet();
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new TermFrequencyQueryTransformer(settings, markingFunctions, responseObjectFactory);
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        TermFrequencyQueryConfiguration config = new TermFrequencyQueryConfiguration(this, settings);
        config.setClient(client);
        config.setAuthorizations(auths);

        int pos = settings.getQuery().indexOf(':');
        String term = null;
        if (pos > 0) {
            term = settings.getQuery().substring(pos + 1);
        } else {
            term = settings.getQuery();
        }

        // TODO: Handle other parameters here

        String[] parts = StringUtils.split(term, '/');

        if (parts.length != 3) {
            throw new IllegalArgumentException(
                            "Query does not specify all necessary parts: " + settings.getQuery() + ". Should be of the form 'DOCUMENT:shardId/datatype/uid'.");
        } else {
            String shardId = parts[0];
            String datatype = parts[1];
            String uid = parts[2];

            log.debug("Received identifier: " + shardId + "," + datatype + "," + uid);

            String END = PARENT_ONLY;
            Parameter p = settings.findParameter(QueryParameters.CONTENT_VIEW_ALL);
            if (p != null && Boolean.parseBoolean(p.getParameterValue())) {
                END = ALL;
            }

            final String tf = ExtendedDataTypeHandler.TERM_FREQUENCY_COLUMN_FAMILY.toString();
            Key startKey = new Key(shardId, tf, datatype + NULL + uid + NULL);
            Key endKey = new Key(shardId, tf, datatype + NULL + uid + END);
            Range r = new Range(startKey, true, endKey, false);

            config.setRange(r);

            log.debug("Setting range: " + r);
        }

        return config;
    }

    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
        if (!configuration.getClass().getName().equals(TermFrequencyQueryConfiguration.class.getName())) {
            throw new QueryException("Did not receive a TermFrequencyQueryConfiguration instance");
        }

        TermFrequencyQueryConfiguration tfConfig = (TermFrequencyQueryConfiguration) configuration;

        try {
            Scanner scanner = QueryScannerHelper.createScanner(tfConfig.getClient(), tfConfig.getTableName(), tfConfig.getAuthorizations(),
                            tfConfig.getQuery());
            scanner.setRange(tfConfig.getRange());

            this.iterator = scanner.iterator();
            this.scanner = scanner;
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Table not found: " + this.getTableName(), e);
        }
    }
}
