package datawave.query.tables.shard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.ResultPostprocessor;

public class ListResultPostprocessor implements ResultPostprocessor {

    private List<ResultPostprocessor> processors = new ArrayList<>();

    public void addProcessor(ResultPostprocessor processor) {
        processors.add(processor);
    }

    @Override
    public void apply(List<Object> results, boolean flushed) {
        for (ResultPostprocessor processor : processors) {
            processor.apply(results, flushed);
        }
    }

    @Override
    public Iterator<Object> flushResults(GenericQueryConfiguration config) {
        // TODO: This could be expensive memory-wise. Consider doing in batches.
        // step through the processors, flushing data from one to the next
        List<Object> resultList = new ArrayList<>();
        for (ResultPostprocessor processor : processors) {
            // if we have any results, then apply
            if (!resultList.isEmpty()) {
                processor.apply(resultList, false);
                processor.saveState(config);
            }
            Iterator<Object> resultIt = processor.flushResults(config);
            while (resultIt.hasNext()) {
                resultList.add(resultIt.next());
            }
        }
        return resultList.iterator();
    }

    @Override
    public void saveState(GenericQueryConfiguration config) {
        for (ResultPostprocessor processor : processors) {
            processor.saveState(config);
        }
    }
}
