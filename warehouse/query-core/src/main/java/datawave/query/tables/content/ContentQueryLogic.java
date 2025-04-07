package datawave.query.tables.content;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.CheckpointableQueryLogic;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl.Parameter;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.config.ContentQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.transformer.ContentQueryTransformer;
import datawave.webservice.query.exception.QueryException;

/**
 * This query table implementation returns a QueryResults object that contains documents from the Shard table. The query will contain the shard id, datatype,
 * and UID of each desired event so that we can seek directly to its respective document. Each document is stored as base64 compressed binary in the Accumulo
 * table. We will decompress the data so that it is base64 encoded binary data in the QueryResults object.
 * <p>
 * The query that needs to be passed to the web service is:
 *
 * <pre>
 *     DOCUMENT:shardId/datatype/uid [DOCUMENT:shardId/datatype/uid]*
 * </pre>
 *
 * The optional parameter content.view.name can be used to retrieve an alternate view of the document, assuming one is stored with that name. The optional
 * parameter content.view.all can be used to retrieve all documents for the parent and children Both optional parameters can be used together
 */
public class ContentQueryLogic extends BaseQueryLogic<Entry<Key,Value>> implements CheckpointableQueryLogic {

    private static final Logger log = Logger.getLogger(ContentQueryLogic.class);

    private static final String PARENT_ONLY = "\1";
    private static final String ALL = "\u10FFFF";

    private int queryThreads = 100;
    ScannerFactory scannerFactory;
    String viewName = null;
    private boolean decodeView = false;

    private ContentQueryConfiguration config;

    public ContentQueryLogic() {
        super();
    }

    public ContentQueryLogic(final ContentQueryLogic contentQueryLogic) {
        super(contentQueryLogic);
        this.queryThreads = contentQueryLogic.queryThreads;
        this.scannerFactory = contentQueryLogic.scannerFactory;
        this.viewName = contentQueryLogic.viewName;
    }

    /**
     * This method calls the base logic's close method, and then attempts to close all batch scanners tracked by the scanner factory, if it is not null.
     */
    @Override
    public void close() {
        super.close();
        final ScannerFactory factory = this.scannerFactory;
        if (null == factory) {
            log.debug("ScannerFactory is null; not closing it.");
        } else {
            int nClosed = 0;
            factory.lockdown();
            for (final ScannerBase bs : factory.currentScanners()) {
                factory.close(bs);
                ++nClosed;
            }
            if (log.isDebugEnabled())
                log.debug("Cleaned up " + nClosed + " batch scanners associated with this query logic.");
        }
    }

    @Override
    public GenericQueryConfiguration initialize(final AccumuloClient client, final Query settings, final Set<Authorizations> auths) throws Exception {
        // Initialize the config and scanner factory
        // NOTE: This needs to set the class-level config object. Do not use a local instance!
        config = new ContentQueryConfiguration(this, settings);
        config.setClient(client);
        config.setAuthorizations(auths);

        this.scannerFactory = new ScannerFactory(config);

        // Re-assign the view name if specified via params
        Parameter p = settings.findParameter(QueryParameters.CONTENT_VIEW_NAME);
        if (null != p && !StringUtils.isEmpty(p.getParameterValue())) {
            this.viewName = p.getParameterValue();
        }

        // Decide whether or not to include the content of child events
        String end;
        p = settings.findParameter(QueryParameters.CONTENT_VIEW_ALL);
        if ((null != p) && (null != p.getParameterValue()) && StringUtils.isNotBlank(p.getParameterValue())) {
            end = ALL;
        } else {
            end = PARENT_ONLY;
        }

        p = settings.findParameter(QueryParameters.DECODE_VIEW);
        if ((null != p) && (null != p.getParameterValue())) {
            this.decodeView = Boolean.parseBoolean(p.getParameterValue());
        }

        // Configure ranges
        final Collection<Range> ranges = this.createRanges(settings, end);
        config.setRanges(ranges);

        return config;
    }

    public void setQueryThreads(int queryThreads) {
        this.queryThreads = queryThreads;
    }

    @Override
    public void setupQuery(GenericQueryConfiguration genericConfig) throws Exception {
        if (!(genericConfig instanceof ContentQueryConfiguration)) {
            throw new QueryException("Did not receive a ContentQueryConfiguration instance!!");
        }

        final ContentQueryConfiguration config = (ContentQueryConfiguration) genericConfig;

        try {
            final BatchScanner scanner = this.scannerFactory.newScanner(config.getTableName(), config.getAuthorizations(), this.queryThreads,
                            config.getQuery());
            scanner.setRanges(config.getRanges());

            if (null != this.viewName) {
                final IteratorSetting cfg = new IteratorSetting(50, this.viewName, RegExFilter.class);
                RegExFilter.setRegexs(cfg, null, null, this.viewName, null, false, true);
                scanner.addScanIterator(cfg);
            }

            this.iterator = scanner.iterator();
            this.scanner = scanner;

        } catch (TableNotFoundException e) {
            throw new RuntimeException("Table not found: " + this.getTableName(), e);
        }
    }

    /*
     * Create an ordered collection of Ranges for scanning
     *
     * @param settings the query
     *
     * @param endKeyTerminator a string appended to each Range's end key indicating whether or not to include child content
     *
     * @return one or more Ranges
     */
    private Collection<Range> createRanges(final Query settings, final String endKeyTerminator) {
        // Initialize the returned collection of ordered ranges
        final Set<Range> ranges = new TreeSet<>();

        // Get the query
        final String query = settings.getQuery().trim();

        int termIndex = 0;
        while (termIndex < query.length()) {
            // Get the next term
            int termSeparation = query.indexOf(' ', termIndex);
            final String term;
            if (termSeparation >= 0) {
                term = query.substring(termIndex, termSeparation + 1).trim();
                termIndex = termSeparation + 1;
            } else {
                term = query.substring(termIndex, query.length()).trim();
                termIndex = query.length();
            }

            // Ignore empty terms
            if (!term.isEmpty()) {
                // Get the next value
                int fieldSeparation = term.indexOf(':');
                final String valueIdentifier;
                if (fieldSeparation > 0) {
                    valueIdentifier = term.substring(fieldSeparation + 1);
                } else {
                    valueIdentifier = term;
                }

                // Remove the identifier if present - we won't use it here, but will extract them from the query
                // later in the ContentQueryTransformer
                int idSeparation = valueIdentifier.indexOf("!");
                final String value = idSeparation > 0 ? valueIdentifier.substring(0, idSeparation) : valueIdentifier;

                // Validate the value
                final String[] parts = value.split("/");
                if (parts.length != 3) {
                    throw new IllegalArgumentException("Query does not specify all needed parts: " + settings.getQuery()
                                    + ". Each space-delimited term should be of the form 'DOCUMENT:shardId/datatype/eventUID'.");
                }
                // Extract the relevant parts of the value and use them to build a content Range
                else {
                    // Get the info necessary to build a content Range
                    final String shardId = parts[0];
                    final String datatype = parts[1];
                    final String uid = parts[2];

                    log.debug("Received pieces: " + shardId + ", " + datatype + ", " + uid);

                    // Create and add a Range
                    final String cf = ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY;
                    final String cq = datatype + Constants.NULL_BYTE_STRING + uid;
                    final Key startKey = new Key(shardId, cf, cq + Constants.NULL_BYTE_STRING);
                    final Key endKey = new Key(shardId, cf, cq + endKeyTerminator);
                    final Range r = new Range(startKey, true, endKey, false);
                    ranges.add(r);

                    log.debug("Adding range: " + r);
                }
            }
        }

        if (ranges.isEmpty()) {
            throw new IllegalArgumentException("Query does not specify all needed parts: " + settings.getQuery()
                            + ". At least one term required of the form 'DOCUMENT:shardId/datatype/eventUID'.");
        }

        return ranges;
    }

    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new ContentQueryTransformer(settings, this.markingFunctions, this.responseObjectFactory, this.decodeView);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new ContentQueryLogic(this);
    }

    public int getQueryThreads() {
        return this.queryThreads;
    }

    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        params.add(QueryParameters.CONTENT_VIEW_NAME);
        params.add(QueryParameters.CONTENT_VIEW_ALL);
        return params;
    }

    @Override
    public Set<String> getRequiredQueryParameters() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getExampleQueries() {
        return Collections.emptySet();
    }

    @Override
    public ContentQueryConfiguration getConfig() {
        if (this.config == null) {
            this.config = ContentQueryConfiguration.create();
        }

        return this.config;
    }

    @Override
    public boolean isCheckpointable() {
        return getConfig().isCheckpointable();
    }

    @Override
    public void setCheckpointable(boolean checkpointable) {
        getConfig().setCheckpointable(checkpointable);
    }

    @Override
    public List<QueryCheckpoint> checkpoint(QueryKey queryKey) {
        if (!isCheckpointable()) {
            throw new UnsupportedOperationException("Cannot checkpoint a query that is not checkpointable.  Try calling setCheckpointable(true) first.");
        }

        // if we have started returning results, then capture the state of the query data objects
        if (this.iterator != null) {
            List<QueryCheckpoint> checkpoints = Lists.newLinkedList();
            for (Range range : ((ContentQueryConfiguration) getConfig()).getRanges()) {
                checkpoints.add(new ContentQueryCheckpoint(queryKey, Collections.singletonList(range)));
            }
            return checkpoints;
        }
        // otherwise we still need to plan or there are no results
        else {
            return Lists.newArrayList(new QueryCheckpoint(queryKey));
        }
    }

    @Override
    public QueryCheckpoint updateCheckpoint(QueryCheckpoint checkpoint) {
        // for the content query logic, the query data objects automatically get updated with
        // the last result returned, so the checkpoint should already be updated!
        return checkpoint;
    }

    @Override
    public void setupQuery(AccumuloClient client, GenericQueryConfiguration config, QueryCheckpoint checkpoint) throws Exception {
        ContentQueryConfiguration contentQueryConfig = (ContentQueryConfiguration) config;
        contentQueryConfig.setRanges(((ContentQueryCheckpoint) checkpoint).getRanges());
        contentQueryConfig.setClient(client);

        scannerFactory = new ScannerFactory(client);

        setupQuery(contentQueryConfig);
    }
}
