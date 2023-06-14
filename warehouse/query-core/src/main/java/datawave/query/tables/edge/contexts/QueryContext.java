package datawave.query.tables.edge.contexts;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import datawave.edge.model.EdgeModelAware;
import datawave.edge.util.EdgeKey;
import datawave.query.tables.edge.EdgeQueryLogic;

import com.google.common.collect.HashMultimap;
import datawave.util.StringUtils;
import org.apache.hadoop.io.Text;

/**
 * A Query context represents a group of ranges over which the same query parameters will be applied The Query context stores 3 data structures: rowContext -
 * used for storing all info needed to build the ranges (a group of sources and sinks) columnContext - holds all the query terms for the column family, column
 * qualifier, as well as any exclusion or function statements otherContexts - holds additional query contexts that do not have any row contexts, used to store
 * some of the more complex query expressions.
 *
 * For example the following query: (EDGE_SOURCE == 's1' OR EDGE_SOURCE == 's2') AND EDGE_SINK == 't1' AND EDGE_ATTRIBUTE2 == 'p1' AND ((EDGE_TYPE == 't1' AND
 * EDGE_RELATIONSHIP == 'r1') OR (EDGE_TYPE =='t2' AND EDGE_RELATIONSHIP == 'r2'))
 *
 * would get stored in the following way: The sources and sinks would be stored in the row context The EDGE_ATTRIBUTE2 would be stored in the column context The
 * two expressions at the end with the edge types/relationships would be two separate query contexts and would be stored in the other contexts list
 *
 * The basic flow of how this class is used is the packageIdentities() method is called in query context depending on what type of identities are being stored
 * it will then send it to the packageIdentities() method in either the rowContext or columnContext
 *
 * The enforce rules boolean that gets passed around is to check to make sure that the same sets of identifiers are not being ANDed together. Eg can't and
 * SOURCE and SOURCE because an edge only has one SOURCE
 */
public class QueryContext implements EdgeModelAware, EdgeContext {
    
    private RowContext rowContext;
    private ColumnContext columnContext;
    private Set<QueryContext> otherContexts;
    
    private boolean hasCompleteColumnFamilies = false;
    
    public void packageIdentities(List<IdentityContext> identityContexts) {
        packageIdentities(identityContexts, true);
    }
    
    public void packageIdentities(List<IdentityContext> identityContexts, boolean enforceRules) {
        if (!identityContexts.get(0).isEquivalence()) {
            buildColumnContexts(identityContexts, enforceRules);
            return;
        }
        String type = identityContexts.get(0).getIdentity();
        
        if (type.equals(EDGE_SOURCE) || type.equals(EDGE_SINK)) {
            buildRowContexts(identityContexts, enforceRules);
        } else if (type.equals(EDGE_TYPE) || type.equals(EDGE_RELATIONSHIP) || type.equals(EDGE_ATTRIBUTE1) || type.equals(EDGE_ATTRIBUTE2)
                        || type.equals(EDGE_ATTRIBUTE3) || type.equals(FUNCTION)) {
            buildColumnContexts(identityContexts, enforceRules);
        } else {
            throw new RuntimeException("Invalid identifier: " + type);
        }
    }
    
    /*
     * If no edge type+relationship is contained in this query context then return false
     */
    public boolean hasCompleteColumnFamily() {
        
        if (columnContext != null) {
            if (columnContext.hasCompleteColumnFamily()) {
                if (otherContexts == null) {
                    hasCompleteColumnFamilies = true;
                    return hasCompleteColumnFamilies;
                } else if (otherContexts != null && allNoColumnFamily()) {
                    hasCompleteColumnFamilies = true;
                    return hasCompleteColumnFamilies;
                }
            } else if (columnContext.hasNoColumnFamily()) {
                if (otherContexts != null && allCompleteColumnFamily()) {
                    hasCompleteColumnFamilies = true;
                    return hasCompleteColumnFamilies;
                }
            }
        } else {
            if (otherContexts != null && allCompleteColumnFamily()) {
                hasCompleteColumnFamilies = true;
                return hasCompleteColumnFamilies;
            }
        }
        
        return hasCompleteColumnFamilies;
        
    }
    
    private boolean allCompleteColumnFamily() {
        for (QueryContext queryContext : otherContexts) {
            if (queryContext.columnContext == null || !queryContext.columnContext.hasCompleteColumnFamily()) {
                return false;
            }
        }
        return true;
    }
    
    private boolean allNoColumnFamily() {
        for (QueryContext queryContext : otherContexts) {
            if (queryContext.columnContext != null && !queryContext.columnContext.hasNoColumnFamily()) {
                return false;
            }
        }
        return true;
    }
    
    /*
     * Should only be called if hasCompleteColumnFamily() == true
     */
    public List<Text> getColumnFamilies(boolean includeStats) {
        if (!hasCompleteColumnFamilies) {
            throw new IllegalStateException("Does not have complete column families");
        }
        
        List<Text> columnFamilies = new ArrayList<>();
        
        if (columnContext != null && columnContext.isCompleteColumnFamilies()) {
            columnFamilies.addAll(columnContext.computeColumnFamilyUnions(includeStats));
        }
        if (otherContexts != null) {
            for (QueryContext queryContext : otherContexts) {
                if (queryContext.columnContext.isCompleteColumnFamilies()) {
                    columnFamilies.addAll(queryContext.columnContext.computeColumnFamilyUnions(includeStats));
                }
                
            }
        }
        return columnFamilies;
    }
    
    /**
     * Attempts to condense several query contexts that all share the same row sources. This current query context may or may not have row id info present (row
     * context). Depending on when this method is used that can be good or bad. (During an AND node this qruey context needs to have sources, during an OR node
     * this query context may or may not have sources)
     *
     * @param other
     *            all query contexts in this must all have row context = null
     * @param optionalSourceInfo
     *            choose to enforce this query context to have its list of sources set or not in order to combine
     * @return Return true if successful. Along the way if a rule is broken return false. If false is returned this query context should no longer be used (it
     *         may have partially combined)
     */
    public boolean combineQueryContexts(List<QueryContext> other, boolean optionalSourceInfo) {
        
        if (optionalSourceInfo || (this.hasSourceList())) {
            // Initialize other contexts if needed
            if (otherContexts == null) {
                otherContexts = new HashSet<>();
            }
            boolean overlappingColumn = false;
            boolean overlappingSink = false;
            
            // Loop over each query context to be combined into this one
            for (QueryContext oContext : other) {
                
                // Cannot have source list
                if (oContext.hasSourceList()) {
                    return false;
                }
                
                // Other contexts should not be maintaining lists of other contexts
                // here we strip the otherContexts of the other Contexts and put them all in the same list
                if (oContext.otherContexts != null) {
                    // loop over the combing query context's other contexts to see if they contain any of the same identity lists that this context has
                    // if so this contexts overlapped identity contexts will need to be moved up into the other context list
                    for (QueryContext qContext : oContext.otherContexts) {
                        if (qContext.columnContext != null) {
                            if (overlappingColumnContext(qContext.columnContext)) {
                                overlappingColumn = true;
                            }
                        }
                        if (qContext.rowContext != null) {
                            if (qContext.rowContext.getSinks() != null && this.rowContext.getSinks() != null) {
                                overlappingSink = true;
                            }
                        }
                        
                        otherContexts.add(qContext);
                    }
                    // clear the pointer so we don't accidentally stumble into them
                    oContext.otherContexts = null;
                }
                
                if (oContext.rowContext != null) {
                    // May need to push this rowContext's list of sinks out into the other contexts
                    if (oContext.rowContext.getSinks() != null && this.rowContext.getSinks() != null) {
                        overlappingSink = true;
                    }
                }
                
                if (oContext.columnContext != null) {
                    // May need to push this columns context out into the other context
                    if (overlappingColumnContext(oContext.columnContext)) {
                        overlappingColumn = true;
                    }
                }
                
                // finally add the combining query context to the list of other context if it has either a non null row or
                // column context. (it could be null if the query context's otherContexts was populated while its row and column
                // context was null)
                if (oContext.columnContext != null || oContext.rowContext != null) {
                    otherContexts.add(oContext);
                }
                
            }
            // If any parts of this queryContext needs to be moved out into the other context list do so here
            if (overlappingColumn || overlappingSink) {
                QueryContext tempContext = new QueryContext();
                // if this query context does not have selector list then if there is an over lap in either the row
                // or column context push them both out into the list of other contexts
                if (!this.hasSourceList() || overlappingSink) {
                    if (this.rowContext != null && this.getRowContext().sinks != null) {
                        tempContext.packageIdentities(this.rowContext.getSinks());
                        this.rowContext.sinks = null;
                    }
                    
                }
                if (this.columnContext != null && (!this.hasSourceList() || overlappingColumn)) {
                    tempContext.setColumnContext(this.columnContext);
                    this.columnContext = null;
                }
                otherContexts.add(tempContext);
            }
            
            return true;
        }
        return false;
    }
    
    /*
     * Helper method used when combining query contexts Checks to see if this column context has any of the same fields set that the other context has.
     */
    private boolean overlappingColumnContext(ColumnContext other) {
        if (this.columnContext == null) {
            return false;
        } else {
            if (this.columnContext.getEdgeTypes() != null && other.getEdgeTypes() != null) {
                return true;
            } else if (this.columnContext.getEdgeRelationships() != null && other.getEdgeRelationships() != null) {
                return true;
            } else if (this.columnContext.getEdgeAttribute1Values() != null && other.getEdgeAttribute1Values() != null) {
                return true;
            } else if (this.columnContext.getEdgeAttribute2Values() != null && other.getEdgeAttribute2Values() != null) {
                return true;
            } else if (this.columnContext.getEdgeAttribute3Values() != null && other.getEdgeAttribute3Values() != null) {
                return true;
            } else if (this.columnContext.getExclusions() != null && other.getExclusions() != null) {
                return true;
            } else if (this.columnContext.getFunctions() != null && other.getFunctions() != null) {
                return true;
            } else {
                return false;
            }
        }
    }
    
    private void buildRowContexts(List<IdentityContext> identityContexts, boolean enforceRules) {
        if (rowContext == null) {
            rowContext = new RowContext();
        }
        rowContext.packageIdentities(identityContexts, enforceRules);
    }
    
    private void buildColumnContexts(List<IdentityContext> identityContexts, boolean enforceRules) {
        if (columnContext == null) {
            columnContext = new ColumnContext();
        }
        columnContext.packageIdentities(identityContexts, enforceRules);
        
    }
    
    private void verifyNotSet(List<IdentityContext> contexts, boolean check) {
        if (check == true && contexts != null) {
            throw new IllegalArgumentException("Can't AND like identifiers: " + contexts.get(0).getIdentity());
        }
    }
    
    /*
     * Takes two string builders to append the query string for this query context. The includStats boolean tells if we need to build the query stats string The
     * includeSource tells if we need to include sources in the query string(s) The includeSink tells if we need to include sinks in the query string(s) we can
     * get away with excluding source and sink if the
     */
    public void buildStrings(StringBuilder normalizedQuery, StringBuilder normalizedStatsQuery, boolean includeStats, boolean includeSource,
                    boolean includeSink, HashMultimap<String,String> preFilterValues, boolean includeColumnFamilyTerms, boolean updateAllowlist) {
        StringBuilder trimmedQuery = new StringBuilder();
        StringBuilder trimmedStatsQuery = new StringBuilder();
        
        NormalizedQuery query = this.toString(includeStats, includeSource, includeSink, preFilterValues, includeColumnFamilyTerms, updateWhitelist);
        
        trimmedQuery.append(query.getNormalizedQuery());
        if (includeStats) {
            trimmedStatsQuery.append(query.getNormalizedStatsQuery());
        }
        
        if (this.otherContexts != null && !otherContexts.isEmpty()) {
            StringBuilder tempQueryString = new StringBuilder();
            StringBuilder tempQueryStatsString = new StringBuilder();
            
            int i = 0;
            for (QueryContext oContext : this.otherContexts) {
                
                if (i > 0) {
                    if (tempQueryString.length() > 7) {
                        tempQueryString.append(OR);
                    }
                    if (includeStats && tempQueryStatsString.length() > 7) {
                        tempQueryStatsString.append(OR);
                    }
                }
                if (this.otherContexts.size() > 1) {
                    tempQueryString.append("(");
                    if (includeStats) {
                        tempQueryStatsString.append("(");
                    }
                }
                query = oContext.toString(includeStats, includeSource, includeSink, preFilterValues, includeColumnFamilyTerms, updateWhitelist);
                tempQueryString.append(query.getNormalizedQuery());
                if (includeStats) {
                    tempQueryStatsString.append(query.getNormalizedStatsQuery());
                }
                
                if (this.otherContexts.size() > 1) {
                    tempQueryString.append(")");
                    if (includeStats) {
                        tempQueryStatsString.append(")");
                    }
                }
                
                i++;
            }
            if (trimmedQuery.length() > 7 && tempQueryString.length() > 7) {
                trimmedQuery.append(AND);
            }
            if (includeStats && trimmedStatsQuery.length() > 7 && tempQueryStatsString.length() > 7) {
                trimmedStatsQuery.append(AND);
            }
            if (tempQueryString.length() > 7) {
                trimmedQuery.append("(" + tempQueryString + ")");
            }
            if (tempQueryStatsString.length() > 7) {
                trimmedStatsQuery.append("(" + tempQueryStatsString + ")");
            }
        }
        normalizedQuery.append(trimmedQuery);
        if (includeStats) {
            normalizedStatsQuery.append(trimmedStatsQuery);
        }
    }
    
    private NormalizedQuery toString(boolean includeStats, boolean includeSource, boolean includeSink, HashMultimap<String,String> preFilterValues,
                    boolean includeColumnFamilyTerms, boolean updateWhitelist) {
        
        NormalizedQuery rowString = null, colString = null;
        
        if (this.getRowContext() != null) {
            rowString = this.getRowContext().toString(includeStats, includeSource, includeSink);
        }
        if (this.getColumnContext() != null) {
            colString = this.getColumnContext().toString(includeStats, preFilterValues, includeColumnFamilyTerms, updateWhitelist);
        }
        
        NormalizedQuery ret = new NormalizedQuery();
        StringBuilder normalizedQuery = new StringBuilder();
        StringBuilder normalizedStatsQuery = new StringBuilder();
        
        if (rowString != null) {
            normalizedQuery.append(rowString.getNormalizedQuery());
            if (includeStats) {
                normalizedStatsQuery.append(rowString.getNormalizedStatsQuery());
            }
        }
        if (colString != null) {
            if (colString.getNormalizedQuery() != null && colString.getNormalizedQuery().length() > 7) {
                if (normalizedQuery.length() > 7) {
                    normalizedQuery.append(AND);
                }
                normalizedQuery.append(colString.getNormalizedQuery());
            }
            if (includeStats && colString.getNormalizedStatsQuery() != null && colString.getNormalizedStatsQuery().length() > 7) {
                if (normalizedStatsQuery.length() > 7) {
                    normalizedStatsQuery.append(AND);
                }
                normalizedStatsQuery.append(colString.getNormalizedStatsQuery());
            }
        }
        
        ret.setNormalizedQuery(normalizedQuery.toString());
        if (includeStats) {
            ret.setNormalizedStatsQuery(normalizedStatsQuery.toString());
        } else {
            ret.setNormalizedStatsQuery("");
        }
        return ret;
    }
    
    public RowContext getRowContext() {
        return rowContext;
    }
    
    public boolean hasSourceList() {
        if (rowContext == null) {
            return false;
        } else {
            if (rowContext.getSources() == null) {
                return false;
            } else {
                return true;
            }
        }
    }
    
    public ColumnContext getColumnContext() {
        return columnContext;
    }
    
    public Set<QueryContext> getOtherContexts() {
        return otherContexts;
    }
    
    private void updateWhiteList(IdentityContext expression, HashMultimap<String,String> preFilterValues) {
        /*
         * A whitelist is a list of things that you allow, therefore, there is no reason to check for things that you do not allow. This means there is no
         * reason to check for NOT_EQUALS or NOT_EQUALS_REGEX, because they won't be allowed by default.
         */
        
        if (expression.getOperation().equals(EQUALS)) {
            preFilterValues.put(expression.getIdentity(), expression.getLiteral());
        } else if (expression.getOperation().equals(EQUALS_REGEX)) {
            preFilterValues.put(expression.getIdentity(), EdgeQueryLogic.PRE_FILTER_DISABLE_KEYWORD);
        }
        
    }
    
    private int populateQuery(List<IdentityContext> terms, StringBuilder trimmedQuery, StringBuilder trimmedStatsQuery, String operator, boolean includeStats,
                    HashMultimap<String,String> preFilterValues, boolean addToPrefilter) {
        int numTermsAdded = 0;
        boolean createStats = includeStats;
        boolean expandStats = false;
        StringBuilder tempStatsStringBuilder = new StringBuilder();
        
        trimmedQuery.append("(");
        if (createStats) {
            tempStatsStringBuilder.append("(");
        }
        
        for (int i = 0; i < terms.size(); i++) {
            IdentityContext iContext = terms.get(i);
            
            if (includeStats == false || iContext.getIdentity().equals(EDGE_SINK)) {
                createStats = false;
            } else {
                createStats = true;
            }
            
            if (iContext.getIdentity().equals(EDGE_RELATIONSHIP) || iContext.getIdentity().equals(EDGE_ATTRIBUTE1)) {
                expandStats = true;
            } else {
                expandStats = false;
            }
            numTermsAdded++;
            if (i > 0) {
                trimmedQuery.append(operator);
                if (createStats && tempStatsStringBuilder.length() > 7) {
                    tempStatsStringBuilder.append(operator);
                }
            }
            
            if (!iContext.getIdentity().equals(FUNCTION)) {
                trimmedQuery.append(iContext.getIdentity() + " " + iContext.getOperation() + " " + "'" + iContext.getEscapedLiteral() + "'");
                if (createStats) {
                    if (expandStats) {
                        tempStatsStringBuilder.append(splitCompoundValue(iContext.getIdentity(), iContext.getOperation(), iContext.getEscapedLiteral(),
                                        preFilterValues, addToPrefilter));
                    } else {
                        tempStatsStringBuilder.append(iContext.getIdentity() + " " + iContext.getOperation() + " " + "'" + iContext.getEscapedLiteral() + "'");
                    }
                }
            } else {
                trimmedQuery.append(iContext.getLiteral());
                if (createStats) {
                    tempStatsStringBuilder.append(iContext.getLiteral());
                }
            }
            
            if (addToPrefilter) {
                updateWhiteList(iContext, preFilterValues);
            }
        }
        createStats = includeStats;
        trimmedQuery.append(")");
        if (createStats) {
            tempStatsStringBuilder.append(")");
        }
        
        if (createStats && tempStatsStringBuilder.length() > 7) {
            if (trimmedStatsQuery.length() > 7) {
                trimmedStatsQuery.append(AND);
            }
            trimmedStatsQuery.append(tempStatsStringBuilder);
        }
        return numTermsAdded;
    }
    
    /*
     * Used for creating the stats query. Splits up EDGE_RELATIONSHIP=A-B into (EDGE_RELATIONSHIP=A)
     */
    private static StringBuilder splitCompoundValue(String name, String operator, String value, HashMultimap<String,String> preFilterValues,
                    boolean updateWhitelist) {
        StringBuilder sb = new StringBuilder();
        
        String[] parts = value.split("-");
        
        // parts should be length 1 or 2 if its not return nothing
        if (parts.length == 1) {
            sb.append(name).append(" " + operator + " '").append(parts[0]).append("'");
        } else if (parts.length == 2) {
            sb.append(name).append(" " + operator + " '").append(parts[0]).append("'");
            // don't need the second value since that would be the sink's relationship and we don't return stats edges for the sink
            
            // If we do split then we need to add the two new values to the pre-filter white list
            if (updateWhitelist) {
                preFilterValues.put(name, parts[0]);
            }
        }
        
        return sb;
        
    }
    
    public class ColumnContext implements EdgeModelAware {
        // Each list (except exclusions and funtions) is expected to have identity contexts all with the same opperation
        private List<IdentityContext> edgeTypes;
        private List<IdentityContext> edgeRelationships;
        private List<IdentityContext> edgeAttribute1Values;
        private List<IdentityContext> edgeAttribute2Values;
        private List<IdentityContext> edgeAttribute3Values;
        private List<IdentityContext> exclusions;
        private List<IdentityContext> functions;
        
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof ColumnContext))
                return false;
            
            ColumnContext that = (ColumnContext) o;
            
            if (completeColumnFamilies != that.completeColumnFamilies)
                return false;
            if (edgeTypes != null ? !edgeTypes.equals(that.edgeTypes) : that.edgeTypes != null)
                return false;
            if (edgeRelationships != null ? !edgeRelationships.equals(that.edgeRelationships) : that.edgeRelationships != null)
                return false;
            if (edgeAttribute1Values != null ? !edgeAttribute1Values.equals(that.edgeAttribute1Values) : that.edgeAttribute1Values != null)
                return false;
            if (edgeAttribute2Values != null ? !edgeAttribute2Values.equals(that.edgeAttribute2Values) : that.edgeAttribute2Values != null)
                return false;
            if (edgeAttribute3Values != null ? !edgeAttribute3Values.equals(that.edgeAttribute3Values) : that.edgeAttribute3Values != null)
                return false;
            if (exclusions != null ? !exclusions.equals(that.exclusions) : that.exclusions != null)
                return false;
            return !(functions != null ? !functions.equals(that.functions) : that.functions != null);
            
        }
        
        @Override
        public int hashCode() {
            int result = edgeTypes != null ? edgeTypes.hashCode() : 0;
            result = 31 * result + (edgeRelationships != null ? edgeRelationships.hashCode() : 0);
            result = 31 * result + (edgeAttribute1Values != null ? edgeAttribute1Values.hashCode() : 0);
            result = 31 * result + (edgeAttribute2Values != null ? edgeAttribute2Values.hashCode() : 0);
            result = 31 * result + (edgeAttribute3Values != null ? edgeAttribute3Values.hashCode() : 0);
            result = 31 * result + (exclusions != null ? exclusions.hashCode() : 0);
            result = 31 * result + (functions != null ? functions.hashCode() : 0);
            result = 31 * result + (completeColumnFamilies ? 1 : 0);
            return result;
        }
        
        private boolean completeColumnFamilies = false;
        
        public void packageIdentities(List<IdentityContext> identityContexts) {
            packageIdentities(identityContexts, true);
        }
        
        public void packageIdentities(List<IdentityContext> identityContexts, boolean enforceRules) {
            if (!identityContexts.get(0).isEquivalence()) {
                addExclusion(identityContexts);
                return;
            }
            
            String type = identityContexts.get(0).getIdentity();
            
            if (type.equals(EDGE_TYPE)) {
                verifyNotSet(edgeTypes, enforceRules);
                addEdgeTypes(identityContexts);
            } else if (type.equals(EDGE_RELATIONSHIP)) {
                verifyNotSet(edgeRelationships, enforceRules);
                addEdgeRelationships(identityContexts);
            } else if (type.equals(EDGE_ATTRIBUTE1)) {
                verifyNotSet(edgeAttribute1Values, enforceRules);
                addEdgeAttribute1Values(identityContexts);
            } else if (type.equals(EDGE_ATTRIBUTE2)) {
                verifyNotSet(edgeAttribute2Values, enforceRules);
                addAttribute2Values(identityContexts);
            } else if (type.equals(EDGE_ATTRIBUTE3)) {
                verifyNotSet(edgeAttribute3Values, enforceRules);
                addAttribute3Values(identityContexts);
            } else if (type.equals(FUNCTION)) {
                verifyNotSet(functions, enforceRules);
                functions = identityContexts;
            } else {
                throw new RuntimeException("Invalid identifier: " + type);
            }
        }
        
        /*
         * Complete column family means has both a edge type list and edge relation list and all operations in both lists are == operations (if =~ it is not
         * complete)
         * 
         * Used to determine if we can set a list of column families to the scanner and omit them from the normalized query string
         */
        public boolean hasCompleteColumnFamily() {
            completeColumnFamilies = true;
            if (edgeTypes == null) {
                completeColumnFamilies = false;
            } else if (edgeTypes.get(0).getOperation() != EQUALS) {
                completeColumnFamilies = false;
            }
            
            if (edgeRelationships == null) {
                completeColumnFamilies = false;
            } else if (edgeRelationships.get(0).getOperation() != EQUALS) {
                completeColumnFamilies = false;
            }
            
            return completeColumnFamilies;
        }
        
        /*
         * no column family mean no edge types and no edge relationships
         * 
         * Used to help determine if we can add a list of column families to the scanner and omit them from the normalized query
         */
        public boolean hasNoColumnFamily() {
            if (edgeTypes == null && edgeRelationships == null) {
                return true;
            } else {
                return false;
            }
            
        }
        
        // This should only be called if hasCompleteColumnFamily() == true
        public List<Text> computeColumnFamilyUnions(boolean includeStats) {
            List<Text> columnFamilies = new ArrayList<>();
            
            for (IdentityContext edgeType : edgeTypes) {
                for (IdentityContext edgeRelationship : edgeRelationships) {
                    
                    columnFamilies.add(new Text(edgeType.getLiteral() + "/" + edgeRelationship.getLiteral()));
                    if (includeStats) {
                        for (EdgeKey.STATS_TYPE stats_type : EdgeKey.STATS_TYPE.values()) {
                            String[] parts = StringUtils.split(edgeRelationship.getLiteral(), '-');
                            columnFamilies.add(new Text("STATS/" + stats_type + "/" + edgeType.getLiteral() + "/" + parts[0]));
                        }
                    }
                }
            }
            
            return columnFamilies;
        }
        
        public NormalizedQuery toString(boolean includeStats, HashMultimap<String,String> preFilterValues, boolean includeColumnFamilyTerms,
                        boolean updateWhitelist) {
            StringBuilder trimmedQuery = new StringBuilder();
            StringBuilder trimmedStatsQuery = new StringBuilder();
            int numTermsAdded = 0;
            
            if (includeColumnFamilyTerms && getEdgeTypes() != null) {
                if (trimmedQuery.length() > 7) {
                    trimmedQuery.append(AND);
                }
                // if (includeStats && trimmedStatsQuery.length() > 7) {trimmedStatsQuery.append(AND);}
                numTermsAdded += populateQuery(getEdgeTypes(), trimmedQuery, trimmedStatsQuery, OR, includeStats, preFilterValues, (updateWhitelist));
            }
            
            if (includeColumnFamilyTerms && getEdgeRelationships() != null) {
                if (trimmedQuery.length() > 7) {
                    trimmedQuery.append(AND);
                }
                // if (includeStats && trimmedStatsQuery.length() > 7) {trimmedStatsQuery.append(AND);}
                numTermsAdded += populateQuery(getEdgeRelationships(), trimmedQuery, trimmedStatsQuery, OR, includeStats, preFilterValues, (updateWhitelist));
            }
            
            if (getEdgeAttribute1Values() != null) {
                if (trimmedQuery.length() > 7) {
                    trimmedQuery.append(AND);
                }
                // if (includeStats && trimmedStatsQuery.length() > 7) {trimmedStatsQuery.append(AND);}
                numTermsAdded += populateQuery(getEdgeAttribute1Values(), trimmedQuery, trimmedStatsQuery, OR, includeStats, preFilterValues, (updateWhitelist));
            }
            
            if (getEdgeAttribute2Values() != null) {
                if (trimmedQuery.length() > 7) {
                    trimmedQuery.append(AND);
                }
                // if (includeStats && trimmedStatsQuery.length() > 7) {trimmedStatsQuery.append(AND);}
                numTermsAdded += populateQuery(getEdgeAttribute2Values(), trimmedQuery, trimmedStatsQuery, OR, includeStats, preFilterValues, (updateWhitelist));
            }
            
            if (getEdgeAttribute3Values() != null) {
                if (trimmedQuery.length() > 7) {
                    trimmedQuery.append(AND);
                }
                // if (includeStats && trimmedStatsQuery.length() > 7) {trimmedStatsQuery.append(AND);}
                numTermsAdded += populateQuery(getEdgeAttribute3Values(), trimmedQuery, trimmedStatsQuery, OR, includeStats, preFilterValues, (updateWhitelist));
            }
            
            if (getExclusions() != null) {
                if (trimmedQuery.length() > 7) {
                    trimmedQuery.append(AND);
                }
                // there could be sinks in this list of exclusions which would not get added do AND'ing in method
                numTermsAdded += populateQuery(getExclusions(), trimmedQuery, trimmedStatsQuery, AND, includeStats, preFilterValues, false);
            }
            
            if (getFunctions() != null) {
                if (trimmedQuery.length() > 7) {
                    trimmedQuery.append(AND);
                }
                // if (includeStats && trimmedStatsQuery.length() > 7) {trimmedStatsQuery.append(AND);}
                numTermsAdded += populateQuery(getFunctions(), trimmedQuery, trimmedStatsQuery, AND, includeStats, preFilterValues, false);
            }
            
            NormalizedQuery ret = new NormalizedQuery();
            if (trimmedQuery.length() > 7) {
                ret.setNormalizedQuery(trimmedQuery.toString());
            } else {
                ret.setNormalizedQuery("");
            }
            if (trimmedStatsQuery.length() > 7) {
                ret.setNormalizedStatsQuery(trimmedStatsQuery.toString());
            } else {
                ret.setNormalizedStatsQuery("");
            }
            
            return ret;
        }
        
        private void addEdgeTypes(List<IdentityContext> identityContexts) {
            if (edgeTypes == null) {
                edgeTypes = identityContexts;
            } else {
                edgeTypes.addAll(identityContexts);
            }
        }
        
        private void addEdgeRelationships(List<IdentityContext> identityContexts) {
            if (edgeRelationships == null) {
                edgeRelationships = identityContexts;
            } else {
                edgeRelationships.addAll(identityContexts);
            }
        }
        
        private void addEdgeAttribute1Values(List<IdentityContext> identityContexts) {
            if (edgeAttribute1Values == null) {
                edgeAttribute1Values = identityContexts;
            } else {
                edgeAttribute1Values.addAll(identityContexts);
            }
        }
        
        private void addAttribute2Values(List<IdentityContext> identityContexts) {
            if (edgeAttribute2Values == null) {
                edgeAttribute2Values = identityContexts;
            } else {
                edgeAttribute2Values.addAll(identityContexts);
            }
        }
        
        private void addAttribute3Values(List<IdentityContext> identityContexts) {
            if (edgeAttribute3Values == null) {
                edgeAttribute3Values = identityContexts;
            } else {
                edgeAttribute3Values.addAll(identityContexts);
            }
        }
        
        /*
         * The list of exclusions is the only list that is allowed to be updated and is the only list that is allowed to have multiple identifier types.
         */
        private void addExclusion(List<IdentityContext> identityContexts) {
            if (exclusions == null) {
                exclusions = identityContexts;
            } else {
                exclusions.addAll(identityContexts);
            }
        }
        
        public boolean isCompleteColumnFamilies() {
            return completeColumnFamilies;
        }
        
        public List<IdentityContext> getEdgeTypes() {
            return edgeTypes;
        }
        
        public List<IdentityContext> getEdgeRelationships() {
            return edgeRelationships;
        }
        
        public List<IdentityContext> getEdgeAttribute1Values() {
            return edgeAttribute1Values;
        }
        
        public List<IdentityContext> getEdgeAttribute2Values() {
            return edgeAttribute2Values;
        }
        
        public List<IdentityContext> getEdgeAttribute3Values() {
            return edgeAttribute3Values;
        }
        
        public List<IdentityContext> getExclusions() {
            return exclusions;
        }
        
        public List<IdentityContext> getFunctions() {
            return functions;
        }
    }
    
    public class RowContext implements EdgeModelAware {
        
        private List<IdentityContext> sources;
        private List<IdentityContext> sinks;
        
        public void packageIdentities(List<IdentityContext> identityContexts) {
            packageIdentities(identityContexts, true);
        }
        
        public void packageIdentities(List<IdentityContext> identityContexts, boolean enforceRules) {
            String type = identityContexts.get(0).getIdentity();
            
            if (type.equals(EDGE_SOURCE)) {
                verifyNotSet(sources, enforceRules);
                addSources(identityContexts);
            } else if (type.equals(EDGE_SINK)) {
                verifyNotSet(sinks, enforceRules);
                addSinks(identityContexts);
            } else {
                throw new RuntimeException("Invalid identifier: " + type);
            }
        }
        
        public NormalizedQuery toString(boolean includeStats, boolean includingSources, boolean includingSinks) {
            StringBuilder trimmedQuery = new StringBuilder();
            StringBuilder trimmedStatsQuery = new StringBuilder();
            
            if (includingSources) {
                populateQuery(getSources(), trimmedQuery, trimmedStatsQuery, OR, includeStats, null, false);
            }
            
            if (getSinks() != null && (includingSources || includingSinks)) {
                if (trimmedQuery.length() > 7) {
                    trimmedQuery.append(AND);
                }
                // never add target sources to stats query no need to append
                populateQuery(getSinks(), trimmedQuery, trimmedStatsQuery, OR, includeStats, null, false);
            }
            
            NormalizedQuery ret = new NormalizedQuery();
            if (trimmedQuery.length() > 7) {
                ret.setNormalizedQuery(trimmedQuery.toString());
            } else {
                ret.setNormalizedQuery("");
            }
            if (trimmedStatsQuery.length() > 7) {
                ret.setNormalizedStatsQuery(trimmedStatsQuery.toString());
            } else {
                ret.setNormalizedStatsQuery("");
            }
            
            return ret;
            
        }
        
        private void addSources(List<IdentityContext> identityContexts) {
            if (sources == null) {
                sources = identityContexts;
            } else {
                sources.addAll(identityContexts);
            }
        }
        
        private void addSinks(List<IdentityContext> identityContexts) {
            if (sinks == null) {
                sinks = identityContexts;
            } else {
                sinks.addAll(identityContexts);
            }
        }
        
        public List<IdentityContext> getSources() {
            return sources;
        }
        
        public List<IdentityContext> getSinks() {
            return sinks;
        }
    }
    
    private class NormalizedQuery {
        String normalizedQuery;
        String normalizedStatsQuery;
        
        public String getNormalizedQuery() {
            return normalizedQuery;
        }
        
        public void setNormalizedQuery(String normalizedQuery) {
            this.normalizedQuery = normalizedQuery;
        }
        
        public String getNormalizedStatsQuery() {
            return normalizedStatsQuery;
        }
        
        public void setNormalizedStatsQuery(String normalizedStatsQuery) {
            this.normalizedStatsQuery = normalizedStatsQuery;
        }
    }
    
    private void setRowContext(RowContext rowContext) {
        this.rowContext = rowContext;
    }
    
    private void setColumnContext(ColumnContext columnContext) {
        this.columnContext = columnContext;
    }
    
    private void setOtherContexts(Set<QueryContext> otherContexts) {
        this.otherContexts = otherContexts;
    }
}
