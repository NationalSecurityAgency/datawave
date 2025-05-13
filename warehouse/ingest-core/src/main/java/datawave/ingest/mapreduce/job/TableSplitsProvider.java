package datawave.ingest.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

public class TableSplitsProvider implements SplitsProvider {
    private TableSplitsCache splitsCache;

    @Override
    public void init(Configuration conf) throws IOException {
        if (splitsCache == null) {
            splitsCache = new TableSplitsCache(conf);
        }
    }

    @Override
    public void refreshView() throws IOException {

    }

    @Override
    public SplitsView readView() throws IOException {
        if (splitsCache != null) {
            return splitsCache;
        }


    }

    static class TableSplitsView implements SplitsView {
        private final TableSplitsCache cache;

        TableSplitsView(TableSplitsCache cache) {
            this.cache = cache;
        }

        @Override
        public boolean isEmpty() throws IOException {
            return cache.getSplits().isEmpty();
        }

        @Override
        public Iterator<SplitEntry> getSplits(String tableName) throws IOException {

        }

        public Iterator<SplitEntry> getSplitsAndLocationByTable(String tableName) throws IOException {
            
        }

        @Override
        public SplitEntry lookupSplit(String tableName, Text row) {
            return null;
        }


    }
}
