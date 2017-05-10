package datawave.query.rewrite.jexl.functions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import datawave.query.rewrite.attributes.ValueTuple;

import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

/**
 * NOTE: The JexlFunctionArgumentDescriptorFactory is implemented by GroupingRequiredFilterFunctionsDescriptor. This is kept as a separate class to reduce
 * accumulo dependencies on other jars.
 * 
 **/
@JexlFunctions(descriptorFactory = "datawave.query.rewrite.jexl.functions.GroupingRequiredFilterFunctionsDescriptor")
public class GroupingRequiredFilterFunctions {
    public static final String GROUPING_REQUIRED_FUNCTION_NAMESPACE = "grouping";
    public static final String CASE_SENSITIVE_EXPRESSION = ".*\\(\\?[idmsux]*-[dmsux]*i[idmsux]*\\).*";
    
    protected static Logger log = Logger.getLogger(GroupingRequiredFilterFunctions.class);
    
    /**
     * fieldsAndRegexen will be either a matched set of field/regex pairs, or a matched set of field/regex pairs followed by an integer, in which
     * 
     * @param args
     * @return
     */
    public static Collection<?> getGroupsForMatchesInGroup(Object... args) {
        // this is either '0', or it is the integer value of the last argument
        // when the argumentCount is odd
        Set<String> groups = Sets.newHashSet();
        int positionFromRight = 0;
        if (args.length % 2 != 0) { // it's odd
            Object lastArgument = args[args.length - 1];
            positionFromRight = Integer.parseInt(lastArgument.toString());
        }
        Collection<ValueTuple> leftSideMatches = Collections.emptySet();
        Collection<ValueTuple> matches = Sets.newHashSet();
        Object fieldValue1 = args[0];
        String regex = args[1].toString();
        if (fieldValue1 instanceof Iterable) {
            // cast as Iterable in order to call the right getAllMatches method
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatches((Iterable) fieldValue1, regex);
        } else {
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatches(fieldValue1, regex);
        }
        for (ValueTuple match : leftSideMatches) {
            String matchFieldName = ValueTuple.getFieldName(match);
            // my fieldValue2 will be a collection that looks like [ 600_7.600.7.1:1, 600_7.600.7.2:2, 600_7.600.7.3:1 ]
            // I am only interested in a match on the one that ends with the 'tail' (.2) that I found above
            String tail = EvaluationPhaseFilterFunctions.getMatchToRightOfPeriod(matchFieldName, positionFromRight);
            if (tail != null && tail.length() > 0) {
                groups.add(tail);
            }
            for (int i = 2; i < args.length; i++) {
                
                Object fieldValue3 = args[i];
                if (args[i] instanceof Iterable) {
                    for (Object fv : (Iterable) args[i]) {
                        String fieldName = (String) ValueTuple.getFieldName(fv);
                        if (fieldName.endsWith(tail)) {
                            fieldValue3 = fv;
                            regex = args[i + 1].toString();
                            // includeRegex will return either an emptyCollection, or a SingletonCollection containing
                            // the first match that was found
                            Collection<ValueTuple> rightSideMatches = EvaluationPhaseFilterFunctions.includeRegex(fieldValue3, regex);
                            if (rightSideMatches.size() > 0) {
                                matches.addAll(rightSideMatches);
                                matches.add(match); // add the left side unmodified match
                            }
                        }
                    }
                }
            }
        }
        // if there was a match found at all levels, then the matches.size will be equal to the
        // number of field/regex pairs
        if (args.length == 2) { // maybe they passed in only 2 args, in that case, get the groups for whatever was in the first (only) arg pair
            matches.addAll(leftSideMatches);
        } else if (matches.size() < args.length / 2) { // truncated in case args.length was odd
            matches.clear();
            groups.clear();
        }
        log.debug("getGroupsForMatchesInGroup(" + Arrays.toString(args) + ") returning " + groups);
        return groups;
        
    }
    
    /**
     * fieldsAndRegexen will be either a matched set of field/regex pairs, or a matched set of field/regex pairs followed by an integer, in which
     * 
     * @param args
     * @return
     */
    public static Collection<?> matchesInGroup(Object... args) {
        // this is either '0', or it is the integer value of the last argument
        // when the argumentCount is odd
        int positionFromRight = 0;
        if (args.length % 2 != 0) { // it's odd
            Object lastArgument = args[args.length - 1];
            positionFromRight = Integer.parseInt(lastArgument.toString());
        }
        Collection<ValueTuple> leftSideMatches = Collections.emptySet();
        Collection<ValueTuple> matches = Sets.newHashSet();
        Object fieldValue1 = args[0];
        String regex = args[1].toString();
        if (fieldValue1 instanceof Iterable) {
            // cast as Iterable in order to call the right getAllMatches method
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatches((Iterable) fieldValue1, regex);
        } else {
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatches(fieldValue1, regex);
        }
        for (ValueTuple match : leftSideMatches) {
            String matchFieldName = ValueTuple.getFieldName(match);
            // my fieldValue2 will be a collection that looks like [ 600_7.600.7.1:1, 600_7.600.7.2:2, 600_7.600.7.3:1 ]
            // I am only interested in a match on the one that ends with the 'tail' (.2) that I found above
            String tail = EvaluationPhaseFilterFunctions.getMatchToRightOfPeriod(matchFieldName, positionFromRight);
            
            for (int i = 2; i < args.length; i++) {
                
                Object fieldValue3 = args[i];
                if (args[i] instanceof Iterable) {
                    for (Object fv : (Iterable) args[i]) {
                        String fieldName = (String) ValueTuple.getFieldName(fv);
                        if (fieldName.endsWith(tail)) {
                            fieldValue3 = fv;
                            regex = args[i + 1].toString();
                            // includeRegex will return either an emptyCollection, or a SingletonCollection containing
                            // the first match that was found
                            Collection<ValueTuple> rightSideMatches = EvaluationPhaseFilterFunctions.includeRegex(fieldValue3, regex);
                            if (rightSideMatches.size() > 0) {
                                matches.addAll(rightSideMatches);
                                matches.add(match); // add the left side unmodified match
                            }
                        }
                    }
                }
            }
        }
        // if there was a match found at all levels, then the matches.size will be equal to the
        // number of field/regex pairs
        if (matches.size() < args.length / 2) { // truncated in case args.length was odd
            matches.clear();
        }
        return Collections.unmodifiableCollection(matches);
        
    }
    
    /**
     * args will be either a matched set of field/regex pairs, or a matched set of field/regex pairs followed by an integer, which is the offset from the left
     * 
     * @param args
     * @return
     */
    public static Collection<?> matchesInGroupLeft(Object... args) {
        log.trace("matchesInGroupLeft(" + Arrays.asList(args) + ")");
        // positionFrom is either '0', or it is the integer value of the last argument
        // when the argumentCount is odd
        int positionFromLeft = 0;
        if (args.length % 2 != 0) { // it's odd
            Object lastArgument = args[args.length - 1];
            positionFromLeft = Integer.parseInt(lastArgument.toString());
        }
        Collection<ValueTuple> firstMatches = Collections.emptySet();
        Collection<ValueTuple> matches = Sets.newHashSet();
        Object fieldValue1 = args[0];
        String regex = args[1].toString();
        if (fieldValue1 instanceof Iterable) {
            // cast as Iterable in order to call the right getAllMatches method
            firstMatches = EvaluationPhaseFilterFunctions.getAllMatches((Iterable) fieldValue1, regex);
        } else {
            firstMatches = EvaluationPhaseFilterFunctions.getAllMatches(fieldValue1, regex);
        }
        log.trace("firstMatches = " + firstMatches);
        for (ValueTuple match : firstMatches) {
            String matchFieldName = ValueTuple.getFieldName(match);
            // my firstMatches will be a collection that looks like [NAME.grandparent_0.parent_0.child_0:SANTINO]
            String theFirstMatch = EvaluationPhaseFilterFunctions.getMatchToLeftOfPeriod(matchFieldName, positionFromLeft);
            
            for (int i = 2; i < args.length; i++) {
                
                if (args[i] instanceof Iterable) {
                    for (Object fv : (Iterable) args[i]) {
                        String fieldName = (String) ValueTuple.getFieldName(fv);
                        
                        regex = args[i + 1].toString();
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
                        String theNextMatch = EvaluationPhaseFilterFunctions.getMatchToLeftOfPeriod(fieldName, positionFromLeft);
                        if (theNextMatch != null && theNextMatch.equals(theFirstMatch)) {
                            log.trace("\tfirst match equals the second: " + theFirstMatch + " == " + theNextMatch);
                            matches.addAll(EvaluationPhaseFilterFunctions.includeRegex(fv, regex));
                            matches.add(match);
                        }
                    }
                }
            }
        }
        
        // if there was a match found at all levels, then the matches.size will be equal to the
        // number of field/regex pairs
        if (matches.size() < args.length / 2) { // truncated in case args.length was odd
            matches.clear();
        }
        log.trace("returning matches:" + matches);
        return Collections.unmodifiableCollection(matches);
    }
    
    public static Collection<ValueTuple> atomValuesMatch(Object fieldOne, Object fieldTwo) {
        if (fieldOne instanceof Iterable == false) {
            fieldOne = Collections.singleton(fieldOne);
        }
        if (fieldTwo instanceof Iterable == false) {
            fieldTwo = Collections.singleton(fieldTwo);
        }
        return atomValuesMatch((Iterable<?>) fieldOne, (Iterable<?>) fieldTwo);
    }
    
    /**
     * test that fields have values that match within the same grouping context. QueryIterator will add (then remove later) the grouping context if it was not
     * specified in the original query
     * 
     * @param fieldOneValues
     * @param fieldTwoValues
     * @return
     */
    public static Collection<ValueTuple> atomValuesMatch(Iterable<?> fieldOneValues, Iterable<?> fieldTwoValues) {
        Set<ValueTuple> matches = Sets.newHashSet();
        if (fieldOneValues == null || fieldTwoValues == null)
            return matches;
        for (Object fieldOneValue : fieldOneValues) {
            String groupOne = ValueTuple.getFieldName(fieldOneValue);
            if (groupOne.indexOf(".") != -1) {
                groupOne = groupOne.substring(groupOne.lastIndexOf("."));
            } else {
                groupOne = "";
            }
            String normedOne = ValueTuple.getNormalizedStringValue(fieldOneValue);
            for (Object fieldTwoValue : fieldTwoValues) {
                String groupTwo = ValueTuple.getFieldName(fieldTwoValue);
                if (groupTwo.indexOf(".") != -1) {
                    groupTwo = groupTwo.substring(groupTwo.lastIndexOf("."));
                } else {
                    groupTwo = "";
                }
                String normedTwo = ValueTuple.getNormalizedStringValue(fieldTwoValue);
                if (normedTwo.equals(normedOne) && groupOne.equals(groupTwo)) {
                    matches.add(EvaluationPhaseFilterFunctions.getHitTerm(fieldTwoValue));
                }
            }
        }
        return matches;
    }
}
