package datawave.core.iterators.filter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.util.StringUtils;

public class CsvKeyFilter extends Filter {
    private static Logger log = Logger.getLogger(CsvKeyFilter.class);

    public static final String ALLOWED_OPT = "kf.allowed";
    public static final String KEY_PART_OPT = "kf.part";

    private enum KeyPart {
        ROW, COLF, COLQ
    }

    private KeyPart part;
    private boolean allowAll = false;
    private Set<Text> allowed;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        String csv = options.get(ALLOWED_OPT);
        if (csv != null && !csv.isEmpty()) {
            allowed = new HashSet<>();
            String[] vals = StringUtils.split(csv, ',');
            for (String val : vals) {
                if (log.isTraceEnabled()) {
                    log.trace("Adding value " + val + " to filter.");
                }
                allowed.add(new Text(val));
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("AllowAll is turned on-- returning true for all values.");
            }
            allowAll = true;
        }

        String part = options.get(KEY_PART_OPT);
        if (part == null || part.isEmpty()) {
            this.part = KeyPart.ROW;
        } else {
            if ("colq".equalsIgnoreCase(part)) {
                this.part = KeyPart.COLQ;
            } else if ("colf".equalsIgnoreCase(part)) {
                this.part = KeyPart.COLF;
            } else {
                this.part = KeyPart.ROW;
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Will filter on key part " + this.part);
        }
    }

    private final Text __buf = new Text();

    @Override
    public boolean accept(Key k, Value v) {
        if (allowAll) {
            return true;
        }

        switch (this.part) {
            case ROW:
                k.getRow(__buf);
                break;
            case COLF:
                k.getColumnFamily(__buf);
                break;
            case COLQ:
                k.getColumnQualifier(__buf);
                break;
        }
        return allowed.contains(__buf);
    }

}
