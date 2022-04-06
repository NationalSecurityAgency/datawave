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

public abstract class SchedulerProducer {
    
    public abstract Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scan, MetadataHelperFactory metadataHelperFactory)
                    throws TableNotFoundException;
    
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
    
    public static class Sequential extends SchedulerProducer {
        
        @Override
        public Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelperFactory metadataHelperFactory) {
            return new SequentialScheduler(config, scannerFactory);
        }
    }
}
