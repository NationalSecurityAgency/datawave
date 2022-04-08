package datawave.query.scheduler;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelperFactory;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;

/**
 * Supplies a {@code Scheduler}, either a {@code SequentialScheduler} or a {@code PushdownScheduler} The {@code SchedulerProducer.Pushdown} is coded as the
 * default for {@code ShardQueryLogic} and can be changed in the spring configuration file, or with an explicit call to set a different one.
 */
public abstract class SchedulerProducer {
    
    /**
     *
     * @param config
     * @param scan
     * @param metadataHelperFactory
     * @return the Scheduler
     * @throws TableNotFoundException
     */
    public abstract Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scan, MetadataHelperFactory metadataHelperFactory)
                    throws TableNotFoundException;
    
    /**
     * Supplies a {@code PushdownScheduler}
     */
    public static class Pushdown extends SchedulerProducer {
        
        @Override
        public Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelperFactory metadataHelperFactory)
                        throws TableNotFoundException {
            PushdownScheduler scheduler = new PushdownScheduler(config, scannerFactory, metadataHelperFactory);
            
            Instance instance = config.getConnector().getInstance();
            String tableName = config.getShardTableName();
            String tableId = Tables.getTableId(instance, tableName);
            Credentials credentials = new Credentials(config.getConnector().whoami(), new PasswordToken(config.getAccumuloPassword()));
            TabletLocator tabletLocator = TabletLocator.getLocator(new ClientContext(instance, credentials, AccumuloConfiguration.getDefaultConfiguration()),
                            tableId);
            
            scheduler.setTableId(tableId);
            scheduler.setTabletLocator(tabletLocator);
            return scheduler;
        }
    }
    
    /**
     * Supplies a {@code SequentialScheduler}
     */
    public static class Sequential extends SchedulerProducer {
        
        @Override
        public Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelperFactory metadataHelperFactory) {
            return new SequentialScheduler(config, scannerFactory);
        }
    }
}
