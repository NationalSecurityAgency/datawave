package datawave.ingest.data.tokenize;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;

import datawave.core.ingest.data.tokenize.DefaultTokenSearch;
import datawave.core.ingest.data.tokenize.StandardAnalyzer;
import datawave.core.ingest.data.tokenize.TokenSearch;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.util.ObjectFactory;

public class TokenizationHelper {

    private static final Logger log = Logger.getLogger(TokenizationHelper.class);

    /**
     * Used to track tokenization execution time. It's too expensive to perform a call to System.currentTimeMillis() each time we produce a new token, so spawn
     * a thread that increments a counter every 500ms.
     * <p>
     * The main thread will check the counter value each time it produces a new token and thus track the number of ticks that have elapsed.
     */
    public static class HeartBeatThread extends Thread {
        private static final Logger log = Logger.getLogger(HeartBeatThread.class);

        public static final long INTERVAL = 500; // half second resolution
        public static volatile int counter = 0;
        public static long lastRun;

        static {
            new HeartBeatThread().start();
        }

        private HeartBeatThread() {
            super("HeartBeatThread");
            setDaemon(true);
        }

        public void run() {
            while (true) {
                try {
                    Thread.sleep(INTERVAL);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                // verify that we're exeuting in a timely fashion
                // ..if not warn.
                long currentRun = System.currentTimeMillis();
                long delta = currentRun - lastRun;
                if (delta > (INTERVAL * 1.5)) {
                    log.warn("HeartBeatThread starved for cpu, " + "should execute every " + INTERVAL + " ms, " + "latest: " + delta + " ms.");
                }
                lastRun = currentRun;
                counter++;
            }
        }
    }

    // Used to indicate that there was a case where the tokenizer took too long.
    public static class TokenizerTimeoutException extends IOException {

        private static final long serialVersionUID = 2307696490675641276L;

        public TokenizerTimeoutException(String message) {
            super(message);
        }
    }

    private CharArraySet stopWords;

    public static final String ANALYZER_CLASS = ".analyzer.class";
    private String analyzerClassName = "datawave.core.ingest.data.tokenize.StandardAnalyzer";

    public static final String TOKENIZER_TIME_THRESHOLDS_MSEC = ".tokenizer.time.thresholds.msec";
    private long[] tokenizerTimeThresholds = new long[0];

    public static final String TOKENIZER_TIME_WARN_MSEC = ".tokenizer.time.warn.threshold.msec";
    private long tokenizerTimeWarnThresholdMsec = Long.MAX_VALUE;

    public static final String TOKENIZER_TIME_ERROR_MSEC = ".tokenizer.time.error.threshold.msec";
    private long tokenizerTimeErrorThresholdMsec = Long.MAX_VALUE;

    public static final String TOKENIZER_TIME_THRESHOLD_NAMES = ".tokenizer.time.threshold.names";
    private String[] tokenizerTimeThresholdNames = new String[0];

    public static final String STOP_WORD_LIST = ".stopword.list.file";
    private String stopWordList = "stopwords.txt";

    public static final String TERM_LENGTH_WARNING_LIMIT = ".term.length.warning.limit";
    private int termLengthWarningLimit = 256;

    public static final String TERM_LENGTH_LIMIT = ".term.length.limit";
    private int termLengthLimit = 50;

    public static final String TOKEN_OFFSET_CACHE_MAX_SIZE = ".token.offset.cache.max.size";
    private int tokenOffsetCacheMaxSize = 10000;

    public static final String TERM_LENGTH_MINIMUM = ".term.length.minimum";
    private int termLengthMinimum = 1;

    public static final String VERBOSE_SHARDS = ".verbose.shard.counters";
    private boolean verboseShardCounters = false;

    public static final String VERBOSE_TERM_SIZE = ".verbose.term.size.counters";
    private boolean verboseTermSizeCounters = false;

    public static final String VERBOSE_TERM_INDEX = ".verbose.term.index.counters";
    private boolean verboseTermIndexCounters = false;

    protected static final String SYNONYM_CREATE = ".token.synonyms.create";
    private boolean synonymGenerationEnabled = true;

    public static final String TERM_WORD_TOKENS = ".term.word.tokens.enabled";
    private boolean termWordTokensEnabled = true;

    public static final String DIRTY_WORD_TOKENS = ".dirty.word.tokens.enabled";
    private boolean dirtyWordTokensEnabled = true;

    public static final String FILE_WORD_TOKENS = ".file.word.tokens.enabled";
    private boolean fileWordTokensEnabled = true;

    public static final String URL_WORD_TOKENS = ".url.word.tokens.enabled";
    private boolean urlWordTokensEnabled = true;

    public static final String EMAIL_WORD_TOKENS = ".email.word.tokens.enabled";
    private boolean emailWordTokensEnabled = true;

    public static final String EMAIL_DOMAIN_TOKENS = ".email.domain.word.tokens.enabled";
    private boolean emailDomainTokensEnabled = true;

    public static final String TERM_TYPE_DISALLOWLIST = ".term.type.disallowlist";
    private String[] termTypeDisallowlist = new String[0];

    public static final String INTERFIELD_POSITION_INCREMENT = ".token.interfield.position.increment";
    private int interFieldPositionIncrement = 10;

    public static final String MAX_URL_DECODES = ".token.interfield.position.increment";
    private int maxUrlDecodes = 2;

    public TokenizationHelper(DataTypeHelper helper, Configuration conf) throws IllegalArgumentException {
        analyzerClassName = conf.get(helper.getType().typeName() + ANALYZER_CLASS, analyzerClassName);
        stopWordList = conf.get(helper.getType().typeName() + STOP_WORD_LIST, stopWordList);
        termLengthLimit = conf.getInt(helper.getType().typeName() + TERM_LENGTH_LIMIT, termLengthLimit);
        termLengthMinimum = conf.getInt(helper.getType().typeName() + TERM_LENGTH_MINIMUM, termLengthMinimum);
        termLengthWarningLimit = conf.getInt(helper.getType().typeName() + TERM_LENGTH_WARNING_LIMIT, termLengthWarningLimit);
        tokenOffsetCacheMaxSize = conf.getInt(helper.getType().typeName() + TOKEN_OFFSET_CACHE_MAX_SIZE, tokenOffsetCacheMaxSize);
        synonymGenerationEnabled = conf.getBoolean(helper.getType().typeName() + SYNONYM_CREATE, synonymGenerationEnabled);
        termWordTokensEnabled = conf.getBoolean(helper.getType().typeName() + TERM_WORD_TOKENS, termWordTokensEnabled);
        dirtyWordTokensEnabled = conf.getBoolean(helper.getType().typeName() + DIRTY_WORD_TOKENS, dirtyWordTokensEnabled);
        fileWordTokensEnabled = conf.getBoolean(helper.getType().typeName() + FILE_WORD_TOKENS, fileWordTokensEnabled);
        urlWordTokensEnabled = conf.getBoolean(helper.getType().typeName() + URL_WORD_TOKENS, urlWordTokensEnabled);
        emailWordTokensEnabled = conf.getBoolean(helper.getType().typeName() + EMAIL_WORD_TOKENS, emailWordTokensEnabled);
        emailDomainTokensEnabled = conf.getBoolean(helper.getType().typeName() + EMAIL_DOMAIN_TOKENS, emailDomainTokensEnabled);
        termTypeDisallowlist = conf.getStrings(helper.getType().typeName() + TERM_TYPE_DISALLOWLIST, termTypeDisallowlist);
        verboseShardCounters = conf.getBoolean(helper.getType().typeName() + VERBOSE_SHARDS, verboseShardCounters);
        verboseTermIndexCounters = conf.getBoolean(helper.getType().typeName() + VERBOSE_TERM_INDEX, verboseTermIndexCounters);
        verboseTermSizeCounters = conf.getBoolean(helper.getType().typeName() + VERBOSE_TERM_SIZE, verboseTermSizeCounters);
        tokenizerTimeWarnThresholdMsec = conf.getLong(helper.getType().typeName() + TOKENIZER_TIME_WARN_MSEC, tokenizerTimeWarnThresholdMsec);
        tokenizerTimeErrorThresholdMsec = conf.getLong(helper.getType().typeName() + TOKENIZER_TIME_ERROR_MSEC, tokenizerTimeErrorThresholdMsec);
        interFieldPositionIncrement = conf.getInt(helper.getType().typeName() + INTERFIELD_POSITION_INCREMENT, interFieldPositionIncrement);

        final String nameProp = helper.getType().typeName() + TOKENIZER_TIME_THRESHOLD_NAMES;
        final String threshProp = helper.getType().typeName() + TOKENIZER_TIME_THRESHOLDS_MSEC;
        tokenizerTimeThresholdNames = conf.getStrings(nameProp, tokenizerTimeThresholdNames);
        String[] tokenizerTimeThresholdStrings = conf.getStrings(threshProp, new String[0]);

        if (tokenizerTimeThresholdStrings.length != tokenizerTimeThresholdNames.length) {
            throw new IllegalArgumentException(
                            "Tokenizer time threshold names [" + nameProp + "] must have the same number of entires (" + tokenizerTimeThresholdNames.length
                                            + ") as tokenizer time thresholds [" + threshProp + "], (" + tokenizerTimeThresholds.length + ")");
        }

        int i = 0;
        try {
            tokenizerTimeThresholds = new long[tokenizerTimeThresholdStrings.length];
            for (; i < tokenizerTimeThresholds.length; i++) {
                tokenizerTimeThresholds[i] = Long.parseLong(tokenizerTimeThresholdStrings[i]);
            }
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                            "Could not parse tokenizer time threshld string from [" + threshProp + "] into a long: [" + tokenizerTimeThresholdStrings[i] + "]");
        }

        stopWords = TokenizationHelper.getStopWords(helper, conf);
    }

    public Analyzer getAnalyzer() {
        String analyzerClassName = getAnalyzerClassName();
        try {
            Class<?> analyzerClass = Class.forName(analyzerClassName);
            if (StandardAnalyzer.class.isAssignableFrom(analyzerClass)) {
                // it is a bit of a stretch to insist that subclasses have
                // the same constructor, but this works here.
                StandardAnalyzer sta = (StandardAnalyzer) ObjectFactory.create(analyzerClassName, getStopWords());
                sta.setMaxTokenLength(this.getTermLengthLimit());
                return sta;
            } else {
                return (Analyzer) ObjectFactory.create(analyzerClassName);
            }
        } catch (ClassNotFoundException ex) {
            throw new Error(ex);
        }
    }

    public String getAnalyzerClassName() {
        return analyzerClassName;
    }

    public long[] getTokenizerTimeThresholds() {
        return tokenizerTimeThresholds;
    }

    public String[] getTokenizerTimeThresholdNames() {
        return tokenizerTimeThresholdNames;
    }

    public String getStopWordList() {
        return stopWordList;
    }

    public long getTokenizerTimeWarnThresholdMsec() {
        return tokenizerTimeWarnThresholdMsec;
    }

    public long getTokenizerTimeErrorThresholdMsec() {
        return tokenizerTimeErrorThresholdMsec;
    }

    public int getTermLengthLimit() {
        return termLengthLimit;
    }

    public int getTermLengthMinimum() {
        return termLengthMinimum;
    }

    public int getTermLengthWarningLimit() {
        return termLengthWarningLimit;
    }

    public int getTokenOffsetCacheMaxSize() {
        return tokenOffsetCacheMaxSize;
    }

    public String[] getTermTypeDisallowlist() {
        return termTypeDisallowlist;
    }

    public boolean isTermWordTokensEnabled() {
        return termWordTokensEnabled;
    }

    public void setTermWordTokensEnabled(boolean termWordTokensEnabled) {
        this.termWordTokensEnabled = termWordTokensEnabled;
    }

    public boolean isDirtyWordTokensEnabled() {
        return dirtyWordTokensEnabled;
    }

    public void setDirtyWordTokensEnabled(boolean dirtyWordTokensEnabled) {
        this.dirtyWordTokensEnabled = dirtyWordTokensEnabled;
    }

    public boolean isFileWordTokensEnabled() {
        return fileWordTokensEnabled;
    }

    public void setFileWordTokensEnabled(boolean fileWordTokensEnabled) {
        this.fileWordTokensEnabled = fileWordTokensEnabled;
    }

    public boolean isUrlWordTokensEnabled() {
        return urlWordTokensEnabled;
    }

    public void setUrlWordTokensEnabled(boolean urlWordTokensEnabled) {
        this.urlWordTokensEnabled = urlWordTokensEnabled;
    }

    public boolean isEmailWordTokensEnabled() {
        return emailWordTokensEnabled;
    }

    public void setEmailWordTokensEnabled(boolean emailWordTokensEnabled) {
        this.emailWordTokensEnabled = emailWordTokensEnabled;
    }

    public boolean isEmailDomainTokensEnabled() {
        return emailDomainTokensEnabled;
    }

    public void setEmailDomainTokensEnabled(boolean emailDomainTokensEnabled) {
        this.emailDomainTokensEnabled = emailDomainTokensEnabled;
    }

    public boolean isVerboseShardCounters() {
        return verboseShardCounters;
    }

    public void setVerboseShardCounters(boolean verboseShardCounters) {
        this.verboseShardCounters = verboseShardCounters;
    }

    public boolean isVerboseTermSizeCounters() {
        return verboseTermSizeCounters;
    }

    public void setVerboseTermSizeCounters(boolean verboseTermSizeCounters) {
        this.verboseTermSizeCounters = verboseTermSizeCounters;
    }

    public boolean isVerboseTermIndexCounters() {
        return verboseTermIndexCounters;
    }

    public void setVerboseTermIndexCounters(boolean verboseTermIndexCounters) {
        this.verboseTermIndexCounters = verboseTermIndexCounters;
    }

    public boolean isSynonymGenerationEnabled() {
        return synonymGenerationEnabled;
    }

    public void setSynonymGenerationEnabled(boolean synonymGenerationEnabled) {
        this.synonymGenerationEnabled = synonymGenerationEnabled;
    }

    public CharArraySet getStopWords() {
        return stopWords;
    }

    public int getInterFieldPositionIncrement() {
        return interFieldPositionIncrement;
    }

    public void setInterFieldPositionIncrement(int interFieldPositionIncrement) {
        this.interFieldPositionIncrement = interFieldPositionIncrement;
    }

    public int getMaxUrlDecodes() {
        return maxUrlDecodes;
    }

    public void setMaxUrlDecodes(int maxUrlDecodes) {
        this.maxUrlDecodes = maxUrlDecodes;
    }

    public TokenSearch configureSearchUtil(TokenSearch searchUtil) {
        searchUtil.setDirtyWordTokensEnabled(isDirtyWordTokensEnabled());
        searchUtil.setFileWordTokensEnabled(isFileWordTokensEnabled());
        searchUtil.setUrlWordTokensEnabled(isUrlWordTokensEnabled());
        searchUtil.setTermWordTokensEnabled(isTermWordTokensEnabled());
        searchUtil.setEmailWordTokensEnabled(isEmailWordTokensEnabled());
        searchUtil.setEmailDomainTokensEnabled(isEmailDomainTokensEnabled());
        searchUtil.setMaxURLDecodes(getMaxUrlDecodes());
        return searchUtil;
    }

    public static final CharArraySet getStopWords(DataTypeHelper helper, Configuration conf) {
        String stopWordsFile = conf.get(helper.getType().outputName() + STOP_WORD_LIST);
        if (stopWordsFile == null) {
            stopWordsFile = conf.get(helper.getType().typeName() + STOP_WORD_LIST);
        }

        CharArraySet stopWords = CharArraySet.EMPTY_SET;
        if (stopWordsFile != null) {
            try {
                stopWords = DefaultTokenSearch.getStopWords(stopWordsFile);
            } catch (IOException e1) {
                throw new IllegalStateException("Could not get stop words from " + stopWordsFile, e1);
            }
        } else {
            log.warn("Utilizing default stopword set. Tokenization and indexing may generate unwanted data");
            stopWords = org.apache.lucene.analysis.core.StopAnalyzer.ENGLISH_STOP_WORDS_SET;
        }
        return stopWords;
    }
}
