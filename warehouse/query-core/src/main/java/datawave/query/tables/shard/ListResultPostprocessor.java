package datawave.query.tables.shard;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.ResultPostprocessor;

public class ListResultPostprocessor implements ResultPostprocessor {

    private List<ResultPostprocessor> processors = new ArrayList<>();

    public void addProcessor(ResultPostprocessor processor) {
        processors.add(processor);
    }

    public int numProcessors() {
        return processors.size();
    }

    @Override
    public void apply(List<Object> results, Object newResult) {
        // run through the processors until we have no results left to process or no more processors
        List<Object> initialResults = new ArrayList<>();
        initialResults.addAll(results);
        List<Object> newResults = new ArrayList<>();
        newResults.add(newResult);
        for (ResultPostprocessor processor : processors) {
            // if all results have been gobbled up by processors, then we are done.
            if (newResults.isEmpty()) {
                break;
            }
            // for each new result, pass it through the processor which will either
            // gobble it up, or add it to the initialResults
            for (Object result : newResults) {
                processor.apply(initialResults, result);
            }

            // we have processed all new Results
            newResults.clear();
            // now reset the new results to be the remaining set for the next processor
            newResults.addAll(initialResults);
            // and clear the remaining set
            initialResults.clear();
        }

        results.clear();
        results.addAll(newResults);
    }

    @Override
    public Iterator<Object> flushResults(GenericQueryConfiguration config) {
        return new FlushedResultsIterator(config);
    }

    // flush the results from the first process through to the last
    public class FlushedResultsIterator implements Iterator<Object> {
        private int nextProcessorToFlush = 0;
        private Iterator<Object> flushed = null;
        private Queue<Object> next = new ArrayDeque<>();
        private final GenericQueryConfiguration config;

        public FlushedResultsIterator(GenericQueryConfiguration config) {
            this.config = config;
            populateNext();
        }

        @Override
        public boolean hasNext() {
            return !next.isEmpty();
        }

        @Override
        public Object next() {
            if (!next.isEmpty()) {
                Object returnNext = next.poll();
                if (next.isEmpty()) {
                    populateNext();
                }
                return returnNext;
            }
            return null;
        }

        private void populateNext() {
            while (next.isEmpty() && nextProcessorToFlush < processors.size()) {
                if (flushed == null) {
                    flushed = processors.get(nextProcessorToFlush++).flushResults(config);
                }

                // flush the next result through the processors
                if (flushed.hasNext()) {
                    List<Object> results = new ArrayList<>();
                    results.add(flushed.next());
                    for (int processorIndex = nextProcessorToFlush; !results.isEmpty() && processorIndex < processors.size(); processorIndex++) {
                        List<Object> newResults = new ArrayList<>();
                        for (Object result : results) {
                            processors.get(processorIndex).apply(newResults, result);
                            processors.get(processorIndex).saveState(config);
                        }
                        results.clear();
                        results.addAll(newResults);
                    }
                    next.addAll(results);
                } else {
                    flushed = null;
                }
            }
        }
    }

    @Override
    public void saveState(GenericQueryConfiguration config) {
        for (ResultPostprocessor processor : processors) {
            processor.saveState(config);
        }
    }
}
