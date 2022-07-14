package datawave.query.jexl.functions;

import datawave.query.attributes.ValueTuple;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * NOTE: The JexlFunctionArgumentDescriptorFactory is implemented by GroupingRequiredFilterFunctionsDescriptor. This is kept as a separate class to reduce
 * accumulo dependencies on other jars.
 * 
 **/
@JexlFunctions(descriptorFactory = "datawave.query.jexl.functions.GroupingRequiredFilterFunctionsDescriptor")
public class GroupingRequiredFilterFunctions {
    public static final String GROUPING_REQUIRED_FUNCTION_NAMESPACE = "grouping";
    
    protected static final Logger log = Logger.getLogger(GroupingRequiredFilterFunctions.class);
    
    /**
     * <pre>
     * 'args' will be either a matched set of field/regex pairs, or a matched set of field/regex pairs followed by an index integer,
     * in which case the integer is a zero based value to determine where to split off the 'group'.
     * FOO.1 with a index of '0' will split off '1'
     * FOO.BLAH.ZIP.0 with an index of '2' will split off 'BLAH.ZIP.0'
     * If no index is supplied, the default is 0
     * 
     * The return is a collection of 'groups' that matched: [0, 1, 5] for example.
     * This collection of groups may be passed as the argument to the method getValuesForGroups.
     * For example: AGE.getValuesForGroups(getGroupsForMatchesInGroup(NAME, 'MEADOW', GENDER, 'FEMALE')
     * The return value from that combination will be a collection of ages for the same groups where NAME and GENDER matched
     * the supplied parameters
     * If the return of the getGroupsForMatchesInGroup call was [0, 1, 5] then the values for AGE.0, AGE.1, AGE.5] will be returned
     * </pre>
     * 
     * @param args
     * @return a collection of the groups that matched.
     */
    public static Collection<?> getGroupsForMatchesInGroup(Object... args) {
        // this is either '0', or it is the integer value of the last argument
        // when the argumentCount is odd
        Set<String> groups = new HashSet<>();
        int positionFromRight = 0;
        if (args.length % 2 != 0) { // it's odd
            Object lastArgument = args[args.length - 1];
            positionFromRight = Integer.parseInt(lastArgument.toString());
        }
        Collection<ValueTuple> leftSideMatches;
        Collection<ValueTuple> allMatches = new HashSet<>();
        Object fieldValue1 = args[0];
        String regex = args[1].toString();
        if (fieldValue1 instanceof Iterable) {
            // cast as Iterable in order to call the right getAllMatches method
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatches((Iterable) fieldValue1, regex);
        } else {
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatches(fieldValue1, regex);
        }
        for (ValueTuple currentMatch : leftSideMatches) {
            String matchFieldName = ValueTuple.getFieldName(currentMatch);
            // my fieldValue2 will be a collection that looks like [ AGE.FOO.7.1:1, GENDER.BAZ.7.2:2, NAME.FO.7.3:1 ]
            // I am only interested in a match on the one that ends with the 'context' (.2) that I found above
            String context = EvaluationPhaseFilterFunctions.getMatchToRightOfPeriod(matchFieldName, positionFromRight);
            if (context != null && !context.isEmpty()) {
                groups.add(context);
            }
            for (int i = 2; i < args.length; i++) {
                
                if (args[i] instanceof Iterable) {
                    boolean contextHasMatch = false;
                    for (Object fieldValue : (Iterable) args[i]) {
                        // do not change the value of contextHasMatch from true back to false.
                        if (manageGroupsForMatchesInGroupRemainingArgs(fieldValue, args[i + 1].toString(), context, allMatches, currentMatch)) {
                            contextHasMatch = true;
                        }
                    }
                    if (!contextHasMatch) {
                        groups.remove(context);
                    }
                } else if (args[i] instanceof ValueTuple) {
                    if (!manageGroupsForMatchesInGroupRemainingArgs(args[i], args[i + 1].toString(), context, allMatches, currentMatch)) {
                        groups.remove(context);
                    }
                }
            }
        }
        // if there was a match found at all levels, then the matches.size will be equal to the
        // number of field/regex pairs
        if (args.length == 2) { // maybe they passed in only 2 args, in that case, get the groups for whatever was in the first (only) arg pair
            allMatches.addAll(leftSideMatches);
        } else if (allMatches.size() < args.length / 2) { // truncated in case args.length was odd
            allMatches.clear();
            groups.clear();
        }
        if (log.isTraceEnabled()) {
            log.trace("getGroupsForMatchesInGroup(" + Arrays.toString(args) + ") returning " + groups);
        }
        return groups;
    }
    
    /**
     * helper function for getGroupsForMatchesInGroup.
     * 
     * @param fieldValue
     * @param regex
     * @param context
     * @param allMatches
     * @param currentMatch
     * @return
     */
    private static boolean manageGroupsForMatchesInGroupRemainingArgs(Object fieldValue, String regex, String context, Collection<ValueTuple> allMatches,
                    ValueTuple currentMatch) {
        if (fieldValue != null) {
            String fieldName = ValueTuple.getFieldName(fieldValue);
            boolean contextHasMatch = false;
            if (fieldName.endsWith(context)) {
                // includeRegex will return either an emptyCollection, or a SingletonCollection containing
                // the first match that was found
                Collection<ValueTuple> rightSideMatches = EvaluationPhaseFilterFunctions.includeRegex(fieldValue, regex);
                if (!rightSideMatches.isEmpty()) {
                    allMatches.addAll(rightSideMatches);
                    allMatches.add(currentMatch); // add the left side unmodified match
                    contextHasMatch = true;
                }
            }
            return contextHasMatch;
        } else {
            return false;
        }
    }
    
    /**
     * <pre>
     * 'args' will be either a matched set of field/regex pairs, or a matched set of field/regex pairs followed by an index integer,
     * in which case the integer is a zero based value to determine where to split off the 'group'.
     * FOO.1 with a index of '0' will split off '1'
     * FOO.BLAH.ZIP.0 with an index of '2' will split off 'BLAH.ZIP.0'
     * If no index is supplied, the default is 0
     * </pre>
     *
     * @param args
     *            suuplies field/regex pairs with an optional index as the last arg
     * @return a collection of matches
     */
    public static Collection<?> matchesInGroup(Object... args) {
        // this is either '0', or it is the integer value of the last argument
        // when the argumentCount is odd
        int positionFromRight = 0;
        if (args.length % 2 != 0) { // it's odd
            Object lastArgument = args[args.length - 1];
            positionFromRight = Integer.parseInt(lastArgument.toString());
        }
        Collection<ValueTuple> leftSideMatches;
        Collection<ValueTuple> allMatches = new HashSet<>();
        Object fieldValue1 = args[0];
        String regex = args[1].toString();
        if (fieldValue1 instanceof Iterable) {
            // cast as Iterable in order to call the right getAllMatches method
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatches((Iterable) fieldValue1, regex);
        } else {
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatches(fieldValue1, regex);
        }
        for (ValueTuple currentMatch : leftSideMatches) {
            String matchFieldName = ValueTuple.getFieldName(currentMatch);
            // my fieldValue2 will be a collection that looks like [ AGE.FOO.7.1:1, GENDER.BAZ.7.2:2, NAME.FO.7.3:1 ]
            // I am only interested in a match on the one that ends with the 'tail' (.2) that I found above
            String context = EvaluationPhaseFilterFunctions.getMatchToRightOfPeriod(matchFieldName, positionFromRight);
            
            for (int i = 2; i < args.length; i += 2) {
                
                if (args[i] instanceof Iterable) {
                    for (Object fv : (Iterable) args[i]) {
                        manageMatchesInGroupRemainingArgs(fv, args[i + 1].toString(), context, allMatches, currentMatch);
                    }
                } else if (args[i] instanceof ValueTuple) {
                    manageMatchesInGroupRemainingArgs(args[i], args[i + 1].toString(), context, allMatches, currentMatch);
                }
            }
        }
        // if there was a match found at all levels, then the matches.size will be equal to the
        // number of field/regex pairs
        if (allMatches.size() < args.length / 2) { // truncated in case args.length was odd
            allMatches.clear();
        }
        return Collections.unmodifiableCollection(allMatches);
        
    }
    
    /**
     * helper function for matchesInGroup
     *
     * @param fieldValue
     * @param regex
     * @param context
     * @param allMatches
     * @param currentMatch
     */
    private static void manageMatchesInGroupRemainingArgs(Object fieldValue, String regex, String context, Collection<ValueTuple> allMatches,
                    ValueTuple currentMatch) {
        String fieldName = ValueTuple.getFieldName(fieldValue);
        if (fieldName.endsWith(context)) {
            // includeRegex will return either an emptyCollection, or a SingletonCollection containing
            // the first match that was found
            Collection<ValueTuple> rightSideMatches = EvaluationPhaseFilterFunctions.includeRegex(fieldValue, regex);
            if (!rightSideMatches.isEmpty()) {
                allMatches.addAll(rightSideMatches);
                allMatches.add(currentMatch); // add the left side unmodified match
            }
        }
    }
    
    /**
     * <pre>
     * 'args' will be either a matched set of field/regex pairs, or a matched set of field/regex pairs followed by an index integer,
     * in which case the integer is a zero based value to determine where to split off the 'group'.
     * FOO.1 with a index of '0' will split off '1'
     * FOO.BLAH.ZIP.0 with an index of '2' will split off 'BLAH.ZIP.0'
     * If no index is supplied, the default is 0
     * </pre>
     *
     * @param args
     * @return a collection of matches
     */
    public static Collection<?> matchesInGroupLeft(Object... args) {
        if (log.isTraceEnabled()) {
            log.trace("matchesInGroupLeft(" + Arrays.asList(args) + ")");
        }
        // positionFrom is either '0', or it is the integer value of the last argument
        // when the argumentCount is odd
        int positionFromLeft = 0;
        if (args.length % 2 != 0) { // it's odd
            Object lastArgument = args[args.length - 1];
            positionFromLeft = Integer.parseInt(lastArgument.toString());
        }
        Collection<ValueTuple> firstMatches;
        Collection<ValueTuple> allMatches = new HashSet<>();
        Object fieldValue1 = args[0];
        String regex = args[1].toString();
        if (fieldValue1 instanceof Iterable) {
            // cast as Iterable in order to call the right getAllMatches method
            firstMatches = EvaluationPhaseFilterFunctions.getAllMatches((Iterable) fieldValue1, regex);
        } else {
            firstMatches = EvaluationPhaseFilterFunctions.getAllMatches(fieldValue1, regex);
        }
        if (log.isTraceEnabled()) {
            log.trace("firstMatches = " + firstMatches);
        }
        for (ValueTuple currentMatch : firstMatches) {
            String matchFieldName = ValueTuple.getFieldName(currentMatch);
            // my firstMatches will be a collection that looks like [NAME.grandparent_0.parent_0.child_0:SANTINO]
            String theFirstMatch = EvaluationPhaseFilterFunctions.getMatchToLeftOfPeriod(matchFieldName, positionFromLeft);
            
            for (int i = 2; i < args.length; i += 2) {
                
                if (args[i] instanceof Iterable) {
                    // args[i] is a collection that looks like:
                    // [[NAME.grandparent_0.parent_1.child_0,LUCA,luca],
                    // [NAME.grandparent_0.parent_0.child_2,MICHAEL,michael],
                    // [NAME.grandparent_0.parent_1.child_1,VINCENT,vincent],
                    // [NAME.grandparent_0.parent_0.child_0,SANTINO,santino],
                    // [NAME.grandparent_0.parent_0.child_1,FREDO,fredo],
                    // [NAME.grandparent_0.parent_0.child_3,CONSTANZIA,constanzia]]
                    // let's say that regex is 'FREDO'
                    // Assuming that positionFromLeft is 0, then for each of these, I will consider only the ones that have a match on
                    // grandparent_0.parent_0, then I will see if the name matches my regex (FREDO)
                    // If the positionFromLeft were 1, I would consider all of the above that include grandparent.0, and then
                    // look for a match on FREDO
                    for (Object fieldValue : (Iterable) args[i]) {
                        String fieldName = ValueTuple.getFieldName(fieldValue);
                        // @formatter:off
                        manageMatchesInGroupLeftRemainingArgs(fieldValue,
                                args[i + 1].toString(), // regex
                                allMatches, theFirstMatch,
                                EvaluationPhaseFilterFunctions.getMatchToLeftOfPeriod(fieldName, positionFromLeft), // the next match
                                currentMatch);
                        // @formatter:on
                    }
                } else if (args[i] instanceof ValueTuple) {
                    // args[i] is a ValueTuple that looks like:
                    // [NAME.grandparent_0.parent_0.child_1,FREDO,fredo],
                    // let's say that regex is 'FREDO'
                    // Assuming that positionFromLeft is 0, then for each of these, I will consider only the ones that have a match on
                    // grandparent_0.parent_0, then I will see if the name matches my regex (FREDO)
                    // If the positionFromLeft were 1, I would consider all of the above that include grandparent.0, and then
                    // look for a match on FREDO
                    Object fieldValue = args[i];
                    String fieldName = ValueTuple.getFieldName(fieldValue);
                    // @formatter:off
                    manageMatchesInGroupLeftRemainingArgs(fieldValue,
                            args[i + 1].toString(), // regex
                            allMatches, theFirstMatch,
                            EvaluationPhaseFilterFunctions.getMatchToLeftOfPeriod(fieldName, positionFromLeft), // the next match
                            currentMatch);
                    // @formatter:on
                }
            }
        }
        
        // if there was a match found at all levels, then the matches.size will be equal to the
        // number of field/regex pairs
        if (allMatches.size() < args.length / 2) { // truncated in case args.length was odd
            allMatches.clear();
        }
        if (log.isTraceEnabled()) {
            log.trace("returning matches:" + allMatches);
        }
        return Collections.unmodifiableCollection(allMatches);
    }
    
    private static void manageMatchesInGroupLeftRemainingArgs(Object fieldValue, String regex, Collection<ValueTuple> allMatches, String theFirstMatch,
                    String theNextMatch, ValueTuple currentMatch) {
        
        if (theNextMatch != null && theNextMatch.equals(theFirstMatch)) {
            if (log.isTraceEnabled()) {
                log.trace("\tfirst match equals the second: " + theFirstMatch + " == " + theNextMatch);
            }
            allMatches.addAll(EvaluationPhaseFilterFunctions.includeRegex(fieldValue, regex));
            allMatches.add(currentMatch);
        }
    }
    
    /**
     * test that fields (field names) have values that match within the same grouping context.
     * 
     * @param fields
     * @return a collection of matches
     * */
    public static Collection<ValueTuple> atomValuesMatch(Object... fields) {
        List<Iterable<?>> iterableFields = new ArrayList<>();
        
        for (Object field : fields) {
            if (field instanceof Iterable == false) {
                field = Collections.singleton(field);
            }
            iterableFields.add((Iterable<?>) field);
        }
        return atomValuesMatch(iterableFields.toArray(new Iterable[iterableFields.size()]));
    }
    
    /**
     * test that fields have values that match within the same grouping context.
     * 
     * @param fields
     * @return a collection of matches
     */
    public static Collection<ValueTuple> atomValuesMatch(Iterable<?>... fields) {
        Set<ValueTuple> matches = new HashSet<>();
        if (fields.length == 0 || Arrays.asList(fields).contains(null)) {
            return matches;
        }
        // save off the first member iterable to use its values as the regexen
        Iterable<?> firstFields = fields[0];
        Set<String> normalizedFirstValues = new HashSet<>();
        for (Object field : firstFields) {
            normalizedFirstValues.add(ValueTuple.getNormalizedStringValue(field));
        }
        for (String regex : normalizedFirstValues) {
            List<Object> argsList = new ArrayList();
            for (int i = 0; i < fields.length; i++) {
                Iterable<?> nextFields = fields[i];
                argsList.add(nextFields);
                argsList.add(regex);
            }
            if (log.isTraceEnabled()) {
                log.trace("argsList:" + argsList);
            }
            Collection<ValueTuple> migMatches = (Collection<ValueTuple>) matchesInGroup(argsList.toArray());
            if (log.isTraceEnabled()) {
                log.trace("migMatches:" + migMatches);
            }
            matches.addAll(migMatches);
        }
        
        if (log.isTraceEnabled()) {
            log.trace("matches:" + matches);
        }
        return matches;
    }
}
