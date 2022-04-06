package datawave.query.scheduler;

import datawave.accumulo.inmemory.impl.InMemoryTabletLocator;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelperFactory;

public abstract class TestSchedulerProducer extends SchedulerProducer {

    public static class Pushdown extends TestSchedulerProducer {
        
        public Pushdown() {
            super();
        }
        
        @Override
        public Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelperFactory metadataHelperFactory) {
            
            PushdownScheduler scheduler = new PushdownScheduler(config, scannerFactory, metadataHelperFactory);
            scheduler.setTableId(config.getIndexTableName());
            scheduler.setTabletLocator(new InMemoryTabletLocator());
            return scheduler;
        }
    }
    
    public static class Sequential extends TestSchedulerProducer {
        
        public Sequential() {
            super();
        }
        
        @Override
        public Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelperFactory metadataHelperFactory) {
            return new SequentialScheduler(config, scannerFactory);
        }
    }
}
