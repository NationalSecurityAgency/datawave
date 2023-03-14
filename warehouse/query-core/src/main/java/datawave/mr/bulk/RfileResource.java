package datawave.mr.bulk;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.query.tables.AccumuloResource;
import datawave.query.tables.BatchResource;
import datawave.query.tables.SessionOptions;

import datawave.webservice.common.connection.WrappedAccumuloClient;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class RfileResource extends BatchResource {
    
    private static final Logger log = Logger.getLogger(RfileResource.class);
    
    Configuration conf;
    
    protected RfileResource(AccumuloClient client) {
        super(client);
    }
    
    public RfileResource(AccumuloResource copy) {
        super(copy);
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
        
        conf = new Configuration();
        
        AccumuloClient client = getClient();
        if (client instanceof WrappedAccumuloClient) {
            client = ((WrappedAccumuloClient) client).getReal();
        }
        
        Properties clientProperties = client.properties();
        final String instanceName = clientProperties.getProperty(ClientProperty.INSTANCE_NAME.getKey());
        final String zookeepers = clientProperties.getProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey());
        
        AccumuloHelper.setInstanceName(conf, instanceName);
        AccumuloHelper.setUsername(conf, client.whoami());
        
        AccumuloHelper.setZooKeepers(conf, zookeepers);
        BulkInputFormat.setZooKeeperInstance(conf, instanceName, zookeepers);
        
        conf.set(MultiRfileInputformat.CACHE_METADATA, "true");
        
        baseScanner = new RfileScanner(client, conf, tableName, auths, 1);
        ((RfileScanner) baseScanner).setRanges(currentRange);
        
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
        super.setOptions(options);
        
        if (log.isDebugEnabled()) {
            log.debug("Setting Options");
        }
        if (null != options.getConfiguration() && null != options.getConfiguration().getAccumuloPassword()) {
            if (log.isDebugEnabled()) {
                log.debug("Setting and configuration");
            }
            AccumuloHelper.setPassword(conf, options.getConfiguration().getAccumuloPassword().getBytes());
            BulkInputFormat.setMemoryInput(conf, getClient().whoami(), options.getConfiguration().getAccumuloPassword().getBytes(), tableName, auths.iterator()
                            .next());
            ((RfileScanner) baseScanner).setConfiguration(conf);
        }
        return this;
    }
    
    @Override
    public String toString() {
        
        //@formatter:off
        String builder = "RFileScanner" + " " +
                "tableName=" + tableName + " " +
                "auths=" + auths + " " +
                "ranges=" + ranges + " ";
        //@formatter:on
        return builder;
        
    }
}
