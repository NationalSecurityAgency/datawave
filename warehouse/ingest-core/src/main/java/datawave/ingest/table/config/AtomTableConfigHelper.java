package datawave.ingest.table.config;

import java.util.EnumSet;
import java.util.HashMap;

import datawave.ingest.data.config.ConfigurationHelper;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class AtomTableConfigHelper extends AbstractTableConfigHelper {

    private static final Logger log = Logger.getLogger(AtomTableConfigHelper.class);
    public static final String ATOM_TTL = "atom.table.ageoff.ms";
    private String tableName;
    private String ageoff;

    @Override
    public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {
        this.tableName = tableName;
        this.ageoff = ConfigurationHelper.isNull(config, ATOM_TTL, String.class);
    }

    @Override
    public void configure(TableOperations tops) {
        if (null != this.ageoff) {
            EnumSet<IteratorScope> scopes = EnumSet.of(IteratorScope.scan, IteratorScope.minc, IteratorScope.majc);
            HashMap<String,String> properties = new HashMap<>();
            properties.put("ttl", ageoff);
            IteratorSetting settings = new IteratorSetting(19, AgeOffFilter.class, properties);

            try {
                tops.attachIterator(tableName, settings, scopes);
            } catch (IllegalArgumentException | AccumuloException iaEx) {
                String msg = iaEx.getMessage();
                if (msg.contains("name conflict"))
                    log.info("Iterator, 'age-off' already exists for table: " + tableName + "\n" + iaEx.getMessage());
                else
                    log.error("Error setting up age-off iterator on table: " + tableName, iaEx);
            } catch (Exception e) {
                log.error("Error setting up age-off iterator on table: " + tableName, e);
            }
        }
    }

}
