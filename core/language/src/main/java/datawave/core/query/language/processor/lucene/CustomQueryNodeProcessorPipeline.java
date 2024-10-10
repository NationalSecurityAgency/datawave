package datawave.core.query.language.processor.lucene;

import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.processors.NoChildOptimizationQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorPipeline;
import org.apache.lucene.queryparser.flexible.core.processors.RemoveDeletedQueryNodesProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.AllowLeadingWildcardProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.BooleanSingleChildOptimizationQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.DefaultPhraseSlopQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.FuzzyQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.MatchAllDocsQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.MultiFieldQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.MultiTermRewriteMethodProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.RemoveEmptyNonLeafQueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.processors.TermRangeQueryNodeProcessor;

public class CustomQueryNodeProcessorPipeline extends QueryNodeProcessorPipeline {
    public CustomQueryNodeProcessorPipeline(QueryConfigHandler configHandler) {
        super(configHandler);

        add(new CustomWildcardQueryNodeProcessor());
        add(new MultiFieldQueryNodeProcessor());
        add(new FuzzyQueryNodeProcessor());
        add(new MatchAllDocsQueryNodeProcessor());
        add(new TermRangeQueryNodeProcessor());
        add(new AllowLeadingWildcardProcessor());
        add(new CustomAnalyzerQueryNodeProcessor());
        add(new NoChildOptimizationQueryNodeProcessor());
        add(new RemoveDeletedQueryNodesProcessor());
        add(new RemoveEmptyNonLeafQueryNodeProcessor());
        add(new BooleanSingleChildOptimizationQueryNodeProcessor());
        add(new DefaultPhraseSlopQueryNodeProcessor());
        add(new MultiTermRewriteMethodProcessor());
        add(new CustomFieldLimiterNodeProcessor());
    }
}
