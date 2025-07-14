package datawave.ingest.data.config.ingest;

import datawave.ingest.data.config.DataTypeHelper;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

public class ErrorShardedIngestHelperTest {


    @Test
    public void setupPopulatesIndexAndReverseIndexSets() throws Exception {

        Configuration conf = new Configuration(false);      // start empty

        ErrorShardedIngestHelper helper = new ErrorShardedIngestHelper();   // concrete class
        helper.setup(conf);


    }
}

