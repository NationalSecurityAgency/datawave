package datawave.query.scheduler;

import datawave.accumulo.inmemory.impl.InMemoryTabletLocator;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelperFactory;

/**
 * Supplies a {@code Scheduler}, either a {@code SequentialScheduler} or a {@code PushdownScehduler} that is configured for unit tests. The
 * {@code TestSchedulerProducer.Pushdown} is codes as the default for {@code ShardQueryLogic} in unit tests, and can be changed in the spring configuration
 * file, or with an explicit call to set a different one.
 */
public abstract class TestSchedulerProducer extends SchedulerProducer {
    
    /**
     * Supplies a {@code PushdownScheduler} that is configured to use the @{code InMemoryTabletLocator} for unit tests
     */
    public static class Pushdown extends TestSchedulerProducer {
        
        @Override
        public Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelperFactory metadataHelperFactory) {
            
            PushdownScheduler scheduler = new PushdownScheduler(config, scannerFactory, metadataHelperFactory);
            scheduler.setTableId(config.getIndexTableName());
            scheduler.setTabletLocator(new InMemoryTabletLocator());
            return scheduler;
        }
    }
    
    /**
     * Supplies a {@code SequentialScheduler}
     */
    public static class Sequential extends TestSchedulerProducer {
        
        @Override
        public Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelperFactory metadataHelperFactory) {
            return new SequentialScheduler(config, scannerFactory);
        }
    }
}
