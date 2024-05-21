package datawave.query.tables.shard;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.ResultPostprocessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ListResultPostprocessor implements ResultPostprocessor {

    private List<ResultPostprocessor> processors = new ArrayList<>();

    public void addProcessor(ResultPostprocessor processor) {
        processors.add(processor);
    }

    @Override
    public void apply(List<Object> results) {
        for (ResultPostprocessor processor : processors) {
            processor.apply(results);
        }
    }

    @Override
    public Iterator<Object> flushResults() {
        // TODO: This could be expensive memory-wise.  Consider doing in batches.
        // step through the processors, flushing data from one to the next
        List<Object> resultList = new ArrayList<>();
        for (ResultPostprocessor processor : processors) {
            processor.apply(resultList);
            Iterator<Object> resultIt = processor.flushResults();
            while(resultIt.hasNext()) {
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
