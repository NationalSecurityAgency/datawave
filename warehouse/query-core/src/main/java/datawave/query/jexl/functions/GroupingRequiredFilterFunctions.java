package datawave.query.jexl.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

import datawave.query.attributes.ValueTuple;

/**
 * NOTE: The {@link JexlFunctionArgumentDescriptorFactory} is implemented by {@link GroupingRequiredFilterFunctionsDescriptor}. This is kept as a separate class
 * to reduce accumulo dependencies on other jars.
 **/
@JexlFunctions(descriptorFactory = "datawave.query.jexl.functions.GroupingRequiredFilterFunctionsDescriptor")
public class GroupingRequiredFilterFunctions {
    public static final String GROUPING_REQUIRED_FUNCTION_NAMESPACE = "grouping";
    private static final Logger log = Logger.getLogger(GroupingRequiredFilterFunctions.class);

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
     *            set of arguments
     * @return a collection of the groups that matched.
     */
    public static Collection<?> getGroupsForMatchesInGroup(Object... args) {
        // this is either '0', or it is the integer value of the last argument
        // when the argumentCount is odd
        Set<String> groups = new HashSet<>();
        final int positionFromRight = (args.length % 2 != 0) ? Integer.parseInt(args[args.length - 1].toString()) : 0;
        Stream<ValueTuple> leftSideMatches;
        Collection<ValueTuple> allMatches = new HashSet<>();
        Object fieldValue1 = args[0];
        String regex = args[1].toString();
        if (fieldValue1 instanceof Iterable) {
            // cast as Iterable in order to call the right getAllMatches method
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatchesStream((Iterable<?>) fieldValue1, regex);
        } else {
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatches(fieldValue1, regex).stream();
        }

        leftSideMatches.forEach(currentMatch -> {
            String matchFieldName = ValueTuple.getFieldName(currentMatch);
            // my fieldValue2 will be a collection that looks like [ AGE.FOO.7.1:1, GENDER.BAZ.7.2:2, NAME.FO.7.3:1 ]
            // I am only interested in a match on the one that ends with the 'context' (.2) that I found above
            String context = EvaluationPhaseFilterFunctions.getMatchToRightOfPeriod(matchFieldName, positionFromRight);
            if (!context.isEmpty()) {
                groups.add(context);
            }
            for (int i = 2; i < args.length; i++) {

                if (args[i] instanceof Iterable) {
                    boolean contextHasMatch = false;
                    for (Object fieldValue : (Iterable<?>) args[i]) {
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
        });

        if (args.length != 2) { // if they passed in only 2 args, then get the groups for whatever was in the first (only) arg pair
            // if there was a match found at all levels, then the matches.size will be equal to the
            // number of field/regex pairs
            if (allMatches.size() < args.length / 2) { // truncated in case args.length was odd
                groups.clear();
            }
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
     *            the field value
     * @param regex
     *            regex string
     * @param context
     *            current context
     * @param allMatches
     *            group of matches
     * @param currentMatch
     *            a current match
     * @return if the context has a match or not
     */
    private static boolean manageGroupsForMatchesInGroupRemainingArgs(Object fieldValue, String regex, String context, Collection<ValueTuple> allMatches,
                    ValueTuple currentMatch) {
        if (fieldValue != null) {
            String fieldName = ValueTuple.getFieldName(fieldValue);
            String subgroup = getSubgroup(fieldName);
            boolean contextHasMatch = false;
            if (subgroup != null && subgroup.equals(context)) {
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

    // move all these kinds of methods into a central utility once we refactor this
    private static String getSubgroup(String fieldName) {
        int index = fieldName.lastIndexOf('.');
        if (index > 0) {
            return fieldName.substring(index + 1);
        }
        return null;

    }

    /**
     * Finds and returns matches across the given arguments. The arguments are expected to be alternating field/regex pairs, optionally followed by a grouping
     * context index integer as the very last argument. The integer will be a zero-based value used to determine where to split off the 'group' from each field.
     * If no index is supplied, a default index of 0 will be used. Examples of splitting off groups:
     * <ul>
     * <li>{@code FOO.1} with an index of 0 will split off {@code '1'}.</li>
     * <li>{@code FOO.BLAH.ZIP.0} with an index of 2 will split off {@code 'BLAH.ZIP.0'}.</li>
     * </ul>
     *
     * @param args
     *            alternating field/regex pairs with an optional index as the last arg
     * @return a collection of matches
     */
    public static Collection<?> matchesInGroup(Object... args) {
        // this is either '0', or it is the integer value of the last argument
        // when the argumentCount is odd
        final int positionFromRight = (args.length % 2 != 0) ? Integer.parseInt(args[args.length - 1].toString()) : 0;
        Stream<ValueTuple> leftSideMatches;
        Collection<ValueTuple> allMatches = new HashSet<>();
        Object fieldValue1 = args[0];
        String regex = args[1].toString();
        if (fieldValue1 instanceof Iterable) {
            // cast as Iterable in order to call the right getAllMatches method
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatchesStream((Iterable) fieldValue1, regex);
        } else {
            leftSideMatches = EvaluationPhaseFilterFunctions.getAllMatches(fieldValue1, regex).stream();
        }

        leftSideMatches.forEach(currentMatch -> {
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
        });

        // if there was a match found at all levels, then the matches.size will be equal to the
        // number of field/regex pairs
        if (allMatches.size() < args.length / 2) { // truncated in case args.length was odd
            allMatches.clear();
        }

        if (log.isTraceEnabled()) {
            log.trace("matchesInGroup(" + Arrays.toString(args) + ") returning " + allMatches);
        }
        return Collections.unmodifiableCollection(allMatches);

    }

    /**
     * helper function for matchesInGroup
     *
     * @param fieldValue
     *            the field value
     * @param regex
     *            regex string
     * @param context
     *            current context
     * @param allMatches
     *            group of matches
     * @param currentMatch
     *            a current match
     */
    private static void manageMatchesInGroupRemainingArgs(Object fieldValue, String regex, String context, Collection<ValueTuple> allMatches,
                    ValueTuple currentMatch) {
        String fieldName = ValueTuple.getFieldName(fieldValue);
        String subgroup = getSubgroup(fieldName);
        if (subgroup != null && subgroup.equals(context)) {
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
     *            matched set of field/regex pairs
     * @return a collection of matches
     */
    public static Collection<?> matchesInGroupLeft(Object... args) {
        if (log.isTraceEnabled()) {
            log.trace("matchesInGroupLeft(" + Arrays.asList(args) + ")");
        }
        // positionFrom is either '0', or it is the integer value of the last argument
        // when the argumentCount is odd
        final int positionFromLeft = (args.length % 2 != 0) ? Integer.parseInt(args[args.length - 1].toString()) : 0;
        Stream<ValueTuple> firstMatches;
        Collection<ValueTuple> allMatches = new HashSet<>();
        Object fieldValue1 = args[0];
        String regex = args[1].toString();
        if (fieldValue1 instanceof Iterable) {
            // cast as Iterable in order to call the right getAllMatches method
            firstMatches = EvaluationPhaseFilterFunctions.getAllMatchesStream((Iterable<?>) fieldValue1, regex);
        } else {
            firstMatches = EvaluationPhaseFilterFunctions.getAllMatches(fieldValue1, regex).stream();
        }
        if (log.isTraceEnabled()) {
            log.trace("firstMatches = " + firstMatches);
        }
        firstMatches.forEach(currentMatch -> {
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
                    for (Object fieldValue : (Iterable<?>) args[i]) {
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
        });

        // if there was a match found at all levels, then the matches.size will be equal to the
        // number of field/regex pairs
        if (allMatches.size() < args.length / 2) { // truncated in case args.length was odd
            allMatches.clear();
        }
        if (log.isTraceEnabled()) {
            log.trace("matchesInGroupLeft(" + args + ") returning " + allMatches);
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
     * Delegates to {@link #atomValuesMatch(Iterable[])} after converting any non-iterable args to a singleton collection containing the original arg.
     *
     * @param fields
     *            the fields to match on
     * @return the matches
     */
    public static Collection<ValueTuple> atomValuesMatch(Object... fields) {
        List<Iterable<?>> iterableFields = new ArrayList<>();
        for (Object field : fields) {
            if (!(field instanceof Iterable)) {
                field = Collections.singleton(field);
            }
            iterableFields.add((Iterable<?>) field);
        }
        return atomValuesMatch(iterableFields.toArray(new Iterable[0]));
    }

    /**
     * Examines and returns any matches across each of the given iterable args. A match is considered such for a field if that field is found in each iterable
     * with the same value and the same grouping context. To optimize matching performance, the first argument should be the smallest iterable.
     *
     * @param fields
     *            the vararg iterables of fields
     * @return the matches
     */
    public static Collection<ValueTuple> atomValuesMatch(Iterable<?>... fields) {
        Set<ValueTuple> matches = new HashSet<>();
        if (fields.length == 0 || Arrays.asList(fields).contains(null)) {
            return matches;
        }

        // Look for matches in all given iterables against each normalized field value found in the first iterable of fields.
        for (Object field : fields[0]) {
            // The normalized value will be treated as a regex pattern when identifying matches.
            String regex = ValueTuple.getNormalizedStringValue(field);
            // Construct the args list that will be passed to matchesInGroup().
            List<Object> argsList = new ArrayList<>();
            for (Iterable<?> iterable : fields) {
                argsList.add(iterable);
                argsList.add(regex);
            }
            Collection<ValueTuple> migMatches = (Collection<ValueTuple>) matchesInGroup(argsList.toArray());
            matches.addAll(migMatches);
        }

        if (log.isTraceEnabled()) {
            log.trace("atomValuesMatch(" + fields + ") returning " + matches);
        }
        return matches;
    }
}
