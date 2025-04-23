package datawave.query.tables;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.security.util.ScannerHelper;

/**
 * Purpose: Basic scanner resource. Contains the connector from which we will create the scanners.
 *
 * Justification: This class should contain all resources that will be used to identify a given scanner session. The batchScanner can be returned as a
 * BatchScanner resource. ScannerResource is an immutable resource. While we could make them mutable, their purpose is to maintain history of scan sessions so
 * that can do runtime analysis, if desired.
 *
 *
 *
 */
public class RunningResource extends AccumuloResource {

    private static final Logger log = Logger.getLogger(RunningResource.class);

    /**
     * Connected table name.
     */
    protected String tableName;

    /**
     * Authorizations.
     */
    protected Set<Authorizations> auths;

    /**
     * Ranges for this resource.
     */
    protected Collection<Range> ranges;

    /**
     * Internal timer to track progress.
     */
    protected StopWatch internalTimer;

    /**
     * Base scanner.
     */
    protected ScannerBase baseScanner = null;

    /**
     * Available.
     */
    protected boolean available = false;

    protected String password = null;

    /**
     * Hashcode.
     */
    protected int hashCode = 31;

    protected RunningResource(final AccumuloClient client) {
        super(client);
        internalTimer = new StopWatch();
        internalTimer.start();
    }

    public RunningResource(AccumuloResource copy) {
        this(copy.getClient());
    }

    /**
     * Initializes the scanner resource
     *
     * @param auths
     *            the auths
     * @param tableName
     *            a table name
     * @throws TableNotFoundException
     *             if the table was not found
     *
     */
    @Override
    protected void init(final String tableName, final Set<Authorizations> auths, Collection<Range> currentRange) throws TableNotFoundException {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkArgument(null != currentRange && !currentRange.isEmpty());

        // copy the appropriate variables.
        ranges = Lists.newArrayList(currentRange);

        this.tableName = tableName;

        this.auths = Sets.newHashSet(auths);

        if (log.isTraceEnabled())
            log.trace("Creating scanner resource from " + tableName + " " + auths + " " + currentRange);

        internalTimer = new StopWatch();
        internalTimer.start();

        // let's pre-compute the hashcode.
        hashCode += new HashCodeBuilder().append(tableName).append(auths).append(ranges).toHashCode();

        baseScanner = ScannerHelper.createScanner(getClient(), tableName, auths);

        if (baseScanner != null) {
            ((Scanner) baseScanner).setRange(currentRange.iterator().next());
        }
    }

    /**
     * Sets the option on this currently running resource.
     *
     * @param options
     *            the options to set
     * @return the current resource
     */
    @Override
    public AccumuloResource setOptions(SessionOptions options) {
        // set the families
        for (Column family : options.getFetchedColumns()) {
            if (family.columnQualifier != null)
                baseScanner.fetchColumn(new Text(family.columnFamily), new Text(family.columnQualifier));
            else {
                if (log.isTraceEnabled())
                    log.trace("Setting column family " + new Text(family.columnFamily));
                baseScanner.fetchColumnFamily(new Text(family.columnFamily));
            }
        }
        for (IteratorSetting setting : options.getIterators()) {
            if (log.isTraceEnabled())
                log.trace("Adding setting, " + setting);
            baseScanner.addScanIterator(setting);
        }

        if (baseScanner instanceof SessionOptions) {
            SessionOptionsDelegate delegate = new SessionOptionsDelegate(options);

            baseScanner.setConsistencyLevel(delegate.getConsistencyLevel());
            baseScanner.setExecutionHints(delegate.getExecutionHints());
        }

        return this;
    }

    /**
     * Return the iterator for this currently running resource.
     *
     * @return the iterator
     */
    public Iterator<Entry<Key,Value>> iterator() {
        return baseScanner.iterator();
    }

    /**
     * Returns the currently running scan
     *
     * @return current scan
     */
    public ScannerBase getRunningResource() {
        return baseScanner;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {

        if (null != obj && obj instanceof RunningResource) {
            RunningResource other = (RunningResource) obj;
            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(tableName, other.tableName);
            equalsBuilder.append(ranges, ranges);
            equalsBuilder.append(internalTimer.getStartTime(), other.internalTimer.getStartTime());
            return equalsBuilder.isEquals();
        }
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(toString());
        }

        if (null != baseScanner) {
            baseScanner.close();
        } else if (log.isTraceEnabled()) {
            log.trace(toString());

        }

    }

    @Override
    public String toString() {

        StringBuilder builder = new StringBuilder();
        if (null != baseScanner && baseScanner instanceof BatchScanner)
            builder.append("BatchScanner").append(" ");
        else
            builder.append("SingleScanner").append(" ");
        builder.append("tableName=").append(tableName).append(" ");
        builder.append("auths=").append(auths).append(" ");
        builder.append("ranges=").append(ranges).append(" ");
        return builder.toString();

    }

    /**
     * Accumulo's {@link ScannerBase} provides the ability to {@link ScannerBase#setExecutionHints(Map)} but no way to get the execution hints. Remedy that with
     * a little delegating.
     */
    public static class SessionOptionsDelegate extends SessionOptions {

        public SessionOptionsDelegate(SessionOptions options) {
            super(options);
        }

        public Map<String,String> getExecutionHints() {
            return this.executionHints;
        }
    }
}
