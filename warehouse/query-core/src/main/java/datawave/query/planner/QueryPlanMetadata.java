package datawave.query.planner;

/**
 * Lightweight description of an {@link org.apache.commons.jexl2.parser.ASTJexlScript} query tree.
 *
 * Count nodes defined by {@link datawave.query.parser.JexlOperatorConstants}. Further broken down by specific datawave implementaitons.
 *
 * Answers questions like "Does this query plan contain any regex nodes?" or "How many terms are in this query plan?"
 */
public class QueryPlanMetadata {
    
    // Count basic jexl nodes.
    private int orNodeCount = 0; // Or
    private int andNodeCount = 0; // And
    private int eqNodeCount = 0; // Equals
    private int neNodeCount = 0; // Not Equals
    private int erNodeCount = 0; // Regex Equals
    private int nrNodeCount = 0; // Regex Not Equals
    private int ltNodeCount = 0; // Less Than
    private int gtNodeCount = 0; // Greater Than
    private int leNodeCount = 0; // Less Than or Equal
    private int geNodeCount = 0; // Greater Than or Equal
    private int fNodeCount = 0; // Function node count.
    private int notNodeCount = 0; // Not node count.
    
    // Count specific datawave implementations of ASTReference, incremented from ASTIdentifier nodes.
    private int delayedPredicateCount = 0;
    private int evaluationOnlyCount = 0;
    private int indexHoleCount = 0;
    private int exceededOrValueThresholdCount = 0; // ExceededValueThresholdMar
    private int exceededTermThresholdCount = 0;
    private int exceededValueThresholdCount = 0;
    
    // Count filter functions
    private int includeRegexCount = 0;
    private int excludeRegexCount = 0;
    private int isNullCount = 0;
    private int betweenDatesCount = 0;
    private int betweenLoadDatesCount = 0;
    private int matchesAtLeastCount = 0;
    private int timeFunctionCount = 0;
    private int includeTextCount = 0;
    
    // Special case.
    private int anyFieldCount = 0;
    
    // TODO -- Content functions
    
    // TODO -- Geo/GeoWave functions
    
    // TODO -- Grouping functions
    
    public QueryPlanMetadata() {
        
    }
    
    /*
     * Metadata methods.
     */
    
    // Does this query plan contain OR nodes?
    public boolean hasOrNodes() {
        return orNodeCount > 0;
    }
    
    // Does this query plan contain AND nodes?
    public boolean hasAndNodes() {
        return andNodeCount > 0;
    }
    
    // Does this query plan contain EQUALS nodes?
    public boolean hasEqNodes() {
        return eqNodeCount > 0;
    }
    
    // Does this query plan contain NOT EQUALS nodes?
    public boolean hasNeNodes() {
        return neNodeCount > 0;
    }
    
    // Does this query plan contain REGEX EQUALS nodes?
    public boolean hasRegexEqualsNodes() {
        return erNodeCount > 0;
    }
    
    // Does this query plan contain REGEX NOT EQUALS nodes?
    public boolean hasRegexNotEqualsNodes() {
        return nrNodeCount > 0;
    }
    
    // Does ANY type of regex node exist?
    public boolean hasRegexNodes() {
        return erNodeCount > 0 || nrNodeCount > 0;
    }
    
    // Does this query plan contain LESS THAN nodes?
    public boolean hasLessThanNodes() {
        return ltNodeCount > 0;
    }
    
    // Does this query plan contain GREATER THAN nodes?
    public boolean hasGreaterThanNodes() {
        return gtNodeCount > 0;
    }
    
    // Does this query plan contain LESS THAN OR EQUAL nodes?
    public boolean hasLessThanEqualsNodes() {
        return leNodeCount > 0;
    }
    
    // Does this query plan contain GREATER THAN OR EQUAL nodes?
    public boolean hasGreaterThanEqualsNodes() {
        return gtNodeCount > 0;
    }
    
    // Does this query plan contain FUNCTION nodes?
    public boolean hasFunctionNodes() {
        return fNodeCount > 0;
    }
    
    public boolean hasExceededNodes() {
        return exceededOrValueThresholdCount > 0 || exceededTermThresholdCount > 0 || exceededValueThresholdCount > 0;
    }
    
    public boolean hasIncludeRegexFilter() {
        return includeRegexCount > 0;
    }
    
    public boolean hasExcludeRegexFilter() {
        return excludeRegexCount > 0;
    }
    
    public boolean hasIsNullFilter() {
        return isNullCount > 0;
    }
    
    public boolean hasBetweenDatesFilter() {
        return betweenDatesCount > 0;
    }
    
    public boolean hasBetweenLoadDatesFilter() {
        return betweenLoadDatesCount > 0;
    }
    
    public boolean hasMatchesAtLeastFilter() {
        return matchesAtLeastCount > 0;
    }
    
    public boolean hasTimeFunctionFilter() {
        return timeFunctionCount > 0;
    }
    
    public boolean hasIncludeTextFilter() {
        return includeTextCount > 0;
    }
    
    public boolean hasAnyFieldNodes() {
        return anyFieldCount > 0;
    }
    
    // Must have at least one AND, one LT OR LE, and one GT or GE node.
    public boolean boundedRangesPossible() {
        //  @formatter:off
        return andNodeCount > 0 &&
                (gtNodeCount > 0 || geNodeCount > 0) &&
                (ltNodeCount > 0 || leNodeCount > 0);
        //  @formatter:on
    }
    
    /*
     * Increment methods.
     */
    
    public void incrementOrNodeCount() {
        this.orNodeCount++;
    }
    
    public void incrementAndNodeCount() {
        this.andNodeCount++;
    }
    
    public void incrementEqualsNodeCount() {
        this.eqNodeCount++;
    }
    
    public void incrementNotEqualsNodeCount() {
        this.neNodeCount++;
    }
    
    public void incrementRegexEqualsCount() {
        this.erNodeCount++;
    }
    
    public void incrementRegexNotEqualsCount() {
        this.nrNodeCount++;
    }
    
    public void incrementLessThanCount() {
        this.ltNodeCount++;
    }
    
    public void incrementGreaterThanCount() {
        this.gtNodeCount++;
    }
    
    public void incrementLessThanOrEqualsCount() {
        this.leNodeCount++;
    }
    
    public void incrementGreaterThanOrEqualsCount() {
        this.geNodeCount++;
    }
    
    public void incrementFunctionCount() {
        this.fNodeCount++;
    }
    
    public void incrementNotNodeCount() {
        this.notNodeCount++;
    }
    
    public void incrementDelayedPredicateCount() {
        this.delayedPredicateCount++;
    }
    
    public void incrementEvaluationOnlyCount() {
        this.evaluationOnlyCount++;
    }
    
    public void incrementIndexHoldCount() {
        this.indexHoleCount++;
    }
    
    public void incrementExceededOrThresholdCount() {
        this.exceededOrValueThresholdCount++;
    }
    
    public void incrementExceededTermThresholdCount() {
        this.exceededTermThresholdCount++;
    }
    
    public void incrementExceededValueThresholdCount() {
        this.exceededValueThresholdCount++;
    }
    
    public void incrementIncludeRegexCount() {
        this.includeRegexCount++;
    }
    
    public void incrementExcludeRegexCount() {
        this.excludeRegexCount++;
    }
    
    public void incrementIsNullCount() {
        this.isNullCount++;
    }
    
    public void incrementBetweenDatesCount() {
        this.betweenDatesCount++;
    }
    
    public void incrementBetweenLoadDatesCount() {
        this.betweenLoadDatesCount++;
    }
    
    public void incrementMatchesAtLeastCount() {
        this.matchesAtLeastCount++;
    }
    
    public void incrementTimeFunctionCount() {
        this.timeFunctionCount++;
    }
    
    public void incrementIncludeTextCount() {
        this.includeTextCount++;
    }
    
    public void incrementAnyFieldCount() {
        this.anyFieldCount++;
    }
    
    /*
     * Getters/Setters
     */
    
    public int getEqNodeCount() {
        return eqNodeCount;
    }
    
    public void setEqNodeCount(int eqNodeCount) {
        this.eqNodeCount = eqNodeCount;
    }
    
    public int getErNodeCount() {
        return erNodeCount;
    }
    
    public void setErNodeCount(int erNodeCount) {
        this.erNodeCount = erNodeCount;
    }
    
    public int getOrNodeCount() {
        return orNodeCount;
    }
    
    public void setOrNodeCount(int orNodeCount) {
        this.orNodeCount = orNodeCount;
    }
    
    public int getAndNodeCount() {
        return andNodeCount;
    }
    
    public void setAndNodeCount(int andNodeCount) {
        this.andNodeCount = andNodeCount;
    }
    
    public int getNeNodeCount() {
        return neNodeCount;
    }
    
    public void setNeNodeCount(int neNodeCount) {
        this.neNodeCount = neNodeCount;
    }
    
    public int getNrNodeCount() {
        return nrNodeCount;
    }
    
    public void setNrNodeCount(int nrNodeCount) {
        this.nrNodeCount = nrNodeCount;
    }
    
    public int getLtNodeCount() {
        return ltNodeCount;
    }
    
    public void setLtNodeCount(int ltNodeCount) {
        this.ltNodeCount = ltNodeCount;
    }
    
    public int getGtNodeCount() {
        return gtNodeCount;
    }
    
    public void setGtNodeCount(int gtNodeCount) {
        this.gtNodeCount = gtNodeCount;
    }
    
    public int getLeNodeCount() {
        return leNodeCount;
    }
    
    public void setLeNodeCount(int leNodeCount) {
        this.leNodeCount = leNodeCount;
    }
    
    public int getGeNodeCount() {
        return geNodeCount;
    }
    
    public void setGeNodeCount(int geNodeCount) {
        this.geNodeCount = geNodeCount;
    }
    
    public int getFNodeCount() {
        return fNodeCount;
    }
    
    public void setFNodeCount(int fNodeCount) {
        this.fNodeCount = fNodeCount;
    }
    
    public int getNotNodeCount() {
        return notNodeCount;
    }
    
    public void setNotNodeCount(int notNodeCount) {
        this.notNodeCount = notNodeCount;
    }
    
    public int getDelayedPredicateCount() {
        return delayedPredicateCount;
    }
    
    public void setDelayedPredicateCount(int delayedPredicateCount) {
        this.delayedPredicateCount = delayedPredicateCount;
    }
    
    public int getEvaluationOnlyCount() {
        return evaluationOnlyCount;
    }
    
    public void setEvaluationOnlyCount(int evaluationOnlyCount) {
        this.evaluationOnlyCount = evaluationOnlyCount;
    }
    
    public int getIndexHoleCount() {
        return indexHoleCount;
    }
    
    public void setIndexHoleCount(int indexHoleCount) {
        this.indexHoleCount = indexHoleCount;
    }
    
    public int getExceededOrValueThresholdCount() {
        return exceededOrValueThresholdCount;
    }
    
    public void setExceededOrValueThresholdCount(int exceededOrValueThresholdCount) {
        this.exceededOrValueThresholdCount = exceededOrValueThresholdCount;
    }
    
    public int getExceededTermThresholdCount() {
        return exceededTermThresholdCount;
    }
    
    public void setExceededTermThresholdCount(int exceededTermThresholdCount) {
        this.exceededTermThresholdCount = exceededTermThresholdCount;
    }
    
    public int getExceededValueThresholdCount() {
        return exceededValueThresholdCount;
    }
    
    public void setExceededValueThresholdCount(int exceededValueThresholdCount) {
        this.exceededValueThresholdCount = exceededValueThresholdCount;
    }
    
    public int getIncludeRegexCount() {
        return includeRegexCount;
    }
    
    public void setIncludeRegexCount(int includeRegexCount) {
        this.includeRegexCount = includeRegexCount;
    }
    
    public int getExcludeRegexCount() {
        return excludeRegexCount;
    }
    
    public void setExcludeRegexCount(int excludeRegexCount) {
        this.excludeRegexCount = excludeRegexCount;
    }
    
    public int getIsNullCount() {
        return isNullCount;
    }
    
    public void setIsNullCount(int isNullCount) {
        this.isNullCount = isNullCount;
    }
    
    public int getBetweenDatesCount() {
        return betweenDatesCount;
    }
    
    public void setBetweenDatesCount(int betweenDatesCount) {
        this.betweenDatesCount = betweenDatesCount;
    }
    
    public int getBetweenLoadDatesCount() {
        return betweenLoadDatesCount;
    }
    
    public void setBetweenLoadDatesCount(int betweenLoadDatesCount) {
        this.betweenLoadDatesCount = betweenLoadDatesCount;
    }
    
    public int getMatchesAtLeastCount() {
        return matchesAtLeastCount;
    }
    
    public void setMatchesAtLeastCount(int matchesAtLeastCount) {
        this.matchesAtLeastCount = matchesAtLeastCount;
    }
    
    public int getTimeFunctionCount() {
        return timeFunctionCount;
    }
    
    public void setTimeFunctionCount(int timeFunctionCount) {
        this.timeFunctionCount = timeFunctionCount;
    }
    
    public int getIncludeTextCount() {
        return includeTextCount;
    }
    
    public void setIncludeTextCount(int includeTextCount) {
        this.includeTextCount = includeTextCount;
    }
    
    public int getAnyFieldCount() {
        return anyFieldCount;
    }
    
    public void setAnyFieldCount(int anyFieldCount) {
        this.anyFieldCount = anyFieldCount;
    }
    
    public String printAndOrCounts() {
        return "counts { and: " + andNodeCount + ", or: " + orNodeCount + "}";
    }
}
