package datawave.ingest.mapreduce.handler.ssdeep;

import java.util.Collections;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSDeepHashParser {

    private static final Logger log = LoggerFactory.getLogger(SSDeepHashParser.class);

    public Iterator<SSDeepHash> call(String s) throws Exception {
        try {
            return Collections.singletonList(SSDeepHash.parse(s)).iterator();
        } catch (Exception e) {
            log.info("Could not parse SSDeepHash: '" + s + "'");
        }
        return Collections.emptyIterator();
    }
}
