package datawave.query.language.processor.lucene;

import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor;

public class QueryNodeProcessorFactory {
    public QueryNodeProcessor create(QueryConfigHandler configHandler) {
        return new CustomQueryNodeProcessorPipeline(configHandler);
    }
}
