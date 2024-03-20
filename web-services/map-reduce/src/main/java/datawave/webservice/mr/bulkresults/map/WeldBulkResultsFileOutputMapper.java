package datawave.webservice.mr.bulkresults.map;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.jboss.weld.environment.se.Weld;

public class WeldBulkResultsFileOutputMapper extends datawave.core.mapreduce.bulkresults.map.BulkResultsFileOutputMapper {
    private Weld weld;

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper<Key,Value,Key,Value>.Context context) throws IOException, InterruptedException {
        if (System.getProperty("ignore.weld.startMain") == null) {
            System.setProperty("com.sun.jersey.server.impl.cdi.lookupExtensionInBeanManager", "true"); // Disable CDI extensions in Jersey libs

            weld = new Weld("STATIC_INSTANCE");
            weld.initialize();
        }

        super.setup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        if (weld != null) {
            weld.shutdown();
        }
    }
}
