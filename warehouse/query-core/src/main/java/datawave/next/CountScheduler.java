package datawave.next;

import datawave.next.scanner.DocumentScanner;
import datawave.next.scanner.DocumentScheduler;
import datawave.query.config.ShardQueryConfiguration;

public class CountScheduler extends DocumentScheduler {

    public CountScheduler(ShardQueryConfiguration config) {
        super(config);
    }

    @Override
    protected DocumentScanner createScanner() {
        CountScanner scanner = new CountScanner(config, queryDataIterator);
        scanner.setVisitorFunction(visitorFunction);

        // no time like the present
        scanner.start();
        return scanner;
    }
}
