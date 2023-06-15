package datawave.ingest.wikipedia;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Sets;

import datawave.ingest.data.config.DataTypeHelperImpl;
import datawave.util.StringUtils;

/**
 *
 */
public class WikipediaHelper extends DataTypeHelperImpl {

    public static final String WIKIPEDIA_INCLUDE_CONTENT = ".include.full.content";
    private boolean includeContent = true;

    public static final String WIKIPEDIA_TOKENIZER_TIME_WARN_MSEC = ".tokenizer.time.warn.threshold.msec";
    private long tokenizerTimeWarnThresholdMsec = Long.MAX_VALUE;

    public static final String WIKIPEDIA_TOKENIZER_TIME_ERROR_MSEC = ".tokenizer.time.error.threshold.msec";
    private long tokenizerTimeErrorThresholdMsec = Long.MAX_VALUE;

    public static final String WIKIPEDIA_CONTENT_INDEX_FIELDS = ".content.index.fields";
    private Set<String> contentIndexedFields = Sets.newHashSet();

    @Override
    public void setup(Configuration conf) throws IllegalArgumentException {
        super.setup(conf);

        includeContent = conf.getBoolean(this.getType().typeName() + WIKIPEDIA_INCLUDE_CONTENT, includeContent);
        tokenizerTimeWarnThresholdMsec = conf.getLong(this.getType().typeName() + WIKIPEDIA_TOKENIZER_TIME_WARN_MSEC, tokenizerTimeWarnThresholdMsec);
        tokenizerTimeErrorThresholdMsec = conf.getLong(this.getType().typeName() + WIKIPEDIA_TOKENIZER_TIME_ERROR_MSEC, tokenizerTimeErrorThresholdMsec);

        final String contentIndexedFieldsStr = conf.get(this.getType().typeName() + WIKIPEDIA_CONTENT_INDEX_FIELDS, "");
        for (String str : StringUtils.splitIterable(contentIndexedFieldsStr, ',')) {
            contentIndexedFields.add(str.trim());
        }
    }

    public boolean includeContent() {
        return includeContent;
    }

    public long getTokenizerTimeWarnThresholdMsec() {
        return tokenizerTimeWarnThresholdMsec;
    }

    public long getTokenizerTimeErrorThresholdMsec() {
        return tokenizerTimeErrorThresholdMsec;
    }

    public Set<String> getContentIndexedFields() {
        return this.contentIndexedFields;
    }
}
