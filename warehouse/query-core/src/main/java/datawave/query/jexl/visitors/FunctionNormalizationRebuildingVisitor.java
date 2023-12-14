package datawave.query.jexl.visitors;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.jexl3.parser.JexlNodes.newInstanceOfType;
import static org.apache.commons.jexl3.parser.JexlNodes.setChildren;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;

/**
 *
 */
public class FunctionNormalizationRebuildingVisitor extends RebuildingVisitor {
    private static final Logger log = Logger.getLogger(FunctionNormalizationRebuildingVisitor.class);

    private static final NoOpType NO_OP = new NoOpType();

    protected final List<Type<?>> normalizers;
    protected final JexlArgumentDescriptor descriptor;
    protected final MetadataHelper helper;
    protected final Set<String> datatypeFilter;

    protected Boolean normalizationFailed = false;

    public FunctionNormalizationRebuildingVisitor(List<Type<?>> normalizers, JexlArgumentDescriptor descriptor, MetadataHelper helper,
                    Set<String> datatypeFilter) {
        Preconditions.checkNotNull(normalizers);
        Preconditions.checkNotNull(descriptor);
        Preconditions.checkNotNull(helper);
        Preconditions.checkNotNull(datatypeFilter);

        this.normalizers = normalizers;
        this.descriptor = descriptor;
        this.helper = helper;
        this.datatypeFilter = datatypeFilter;
    }

    public static JexlNode normalize(ASTFunctionNode function, Multimap<String,Type<?>> allNormalizers, MetadataHelper helper, Set<String> datatypeFilter) {
        Preconditions.checkNotNull(function);
        Preconditions.checkNotNull(allNormalizers);
        Preconditions.checkNotNull(helper);

        JexlArgumentDescriptor descriptor = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(function);

        List<ASTFunctionNode> functions = Lists.newArrayList();

        // create the distinct normalization lists.
        List<List<Type<?>>> lists = getNormalizerListsForArgs((ASTArguments) function.jjtGetChild(1), allNormalizers, descriptor, helper, datatypeFilter);

        // now for each unique list of normalizers, lets normalize the function arguments
        for (List<Type<?>> list : lists) {
            FunctionNormalizationRebuildingVisitor visitor = new FunctionNormalizationRebuildingVisitor(list, descriptor, helper, datatypeFilter);

            ASTFunctionNode node = (ASTFunctionNode) function.jjtAccept(visitor, null);

            if (!visitor.normalizationFailed) {
                functions.add(node);
            }
        }

        // now reduce the set of functions by eliminating those variants with identical normalized values
        Comparator<ASTFunctionNode> comparator = new ASTFunctionNodeComparator();
        Collections.sort(functions, comparator);
        ASTFunctionNode last = null;
        for (Iterator<ASTFunctionNode> it = functions.iterator(); it.hasNext();) {
            ASTFunctionNode test = it.next();
            if (last != null && comparator.compare(last, test) == 0) {
                it.remove();
            } else {
                last = test;
            }
        }

        if (functions.isEmpty()) {
            return copy(function);
        } else if (1 == functions.size()) {
            return functions.iterator().next();
        } else {
            JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(function);

            if (desc.useOrForExpansion()) {
                return JexlNodeFactory.createOrNode(functions);
            } else {
                return JexlNodeFactory.createAndNode(functions);
            }
        }
    }

    public static class ASTFunctionNodeComparator implements Comparator<ASTFunctionNode> {

        /*
         * (non-Javadoc)
         *
         * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
         */
        @Override
        public int compare(ASTFunctionNode o1, ASTFunctionNode o2) {
            // a function is equivalent if the children are equivalent
            int comparison = o1.jjtGetNumChildren() - o2.jjtGetNumChildren();
            if (comparison == 0) {
                for (int i = 0; i < o1.jjtGetNumChildren(); i++) {
                    comparison = compareChild(o1.jjtGetChild(i), o2.jjtGetChild(i));
                    if (comparison != 0) {
                        break;
                    }
                }
            }
            return comparison;
        }

        protected int compareChild(JexlNode n1, JexlNode n2) {
            int comparison = n1.getClass().getName().compareTo(n2.getClass().getName());
            if (comparison == 0) {
                comparison = n1.jjtGetNumChildren() - n2.jjtGetNumChildren();
                if (comparison == 0) {
                    Object n1Image = JexlNodes.getImage(n1);
                    Object n2Image = JexlNodes.getImage(n2);
                    comparison = (n1Image == null ? (n2Image == null ? 0 : -1)
                                    : (n2Image == null ? 1 : String.valueOf(n1Image).compareTo(String.valueOf(n2Image))));
                    if (comparison == 0) {
                        for (int i = 0; i < n1.jjtGetNumChildren(); i++) {
                            comparison = compareChild(n1.jjtGetChild(i), n2.jjtGetChild(i));
                            if (comparison != 0) {
                                break;
                            }
                        }
                    }
                }
            }
            return comparison;
        }

    }

    /**
     * Create lists of normalizers that maps to the function arguments. Each list is a unique list of normalizers. Each list will be applied to the arguments to
     * produce a separate function instance.
     *
     * @param arguments
     *            the function arguments
     * @param allNormalizers
     *            the normalizers
     * @param descriptor
     *            a descriptor
     * @param helper
     *            a helper
     * @param datatypeFilter
     *            a datatype filter
     * @return the list of normalizer lists
     */
    private static List<List<Type<?>>> getNormalizerListsForArgs(ASTArguments arguments, Multimap<String,Type<?>> allNormalizers,
                    JexlArgumentDescriptor descriptor, MetadataHelper helper, Set<String> datatypeFilter) {
        List<List<Type<?>>> lists = new ArrayList<>();

        lists.add(new ArrayList<>(Arrays.asList(new Type<?>[arguments.jjtGetNumChildren()])));

        // first we go through the arguments and determine those that reference the same field.
        // this is done by assuming that if the fields returned by descriptor.fields is the same
        // for two separate arguments, then those arguments will refer to each field separately.
        // This implies that a cross product of normalizations is NOT required for those groups of
        // arguments. For example a geo function may have one field for lats, and another for lons.
        // for those arguments we want a cross-product of normalizations. However for content functions,
        // all of the arguments will refer to the same set of fields so we assume that the content
        // functions will be applied to each field in turn instead of every combination thereof.
        Map<Set<String>,Set<Integer>> fieldGroups = new HashMap<>();
        Set<String> EMPTY_SET = new HashSet<>();
        for (int i = 0; i < arguments.jjtGetNumChildren(); i++) {
            // get the fields that go with this argument IFF this is a string literal argument
            Set<String> fields = EMPTY_SET;
            List<JexlNode> literals = JexlASTHelper.getLiterals(arguments.jjtGetChild(i));
            if ((literals.size() == 1) && (literals.get(0) instanceof ASTStringLiteral)) {
                fields = new HashSet<>(descriptor.fieldsForNormalization(helper, datatypeFilter, i));
            }
            if (fieldGroups.containsKey(fields)) {
                fieldGroups.get(fields).add(i);
            } else {
                Set<Integer> args = new HashSet<>();
                args.add(i);
                fieldGroups.put(fields, args);
            }
        }

        // now for each group of fields, get the set of normalizers and create the cross-product with
        // the other groups.
        for (Map.Entry<Set<String>,Set<Integer>> argGroup : fieldGroups.entrySet()) {
            // now compile all of the possible normalizers for this argument group
            Set<Type<?>> normalizers = new HashSet<>();
            if (argGroup.getKey().isEmpty()) {
                normalizers.add(NO_OP);
            } else {
                for (String field : argGroup.getKey()) {
                    normalizers.addAll(allNormalizers.get(field));
                }
            }

            // and now expand the lists of normalizers per the normalizers for this argument
            if (normalizers.size() == 1) {
                Type<?> normalizer = normalizers.iterator().next();
                // simply add the normalizer entry to each existing map
                for (List<Type<?>> list : lists) {
                    for (Integer argIndex : argGroup.getValue()) {
                        list.set(argIndex, normalizer);
                    }
                }
            } else {
                // for each normalizer, create a separate copy of the maps
                List<List<Type<?>>> lists2 = new ArrayList<>();
                for (Type<?> normalizer : normalizers) {
                    for (List<Type<?>> list : lists) {
                        List<Type<?>> list2 = new ArrayList<>(list);
                        for (Integer argIndex : argGroup.getValue()) {
                            list2.set(argIndex, normalizer);
                        }
                        lists2.add(list2);
                    }
                }
                lists = lists2;
            }
        }
        return lists;
    }

    @Override
    public Object visit(ASTArguments node, Object data) {
        ASTArguments newNode = newInstanceOfType(node);
        JexlNodes.copyImage(node, newNode);
        ArrayList<JexlNode> children = newArrayList();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode copiedChild = (JexlNode) node.jjtGetChild(i).jjtAccept(this, i);
            if (copiedChild != null) {
                children.add(copiedChild);
            }
        }
        return setChildren(newNode, children.toArray(new JexlNode[0]));
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        // Fail fast
        if (this.normalizationFailed) {
            return node;
        }
        Type<?> normalizer = null;
        // get the argument index
        int index = (Integer) data;

        try {
            normalizer = normalizers.get(index);
            String normalizedValue = (descriptor.regexArguments() ? normalizer.normalizeRegex(node.getLiteral()) : normalizer.normalize(node.getLiteral()));
            ASTStringLiteral normalizedLiteral = JexlNodes.makeStringLiteral();
            JexlNodes.setLiteral(normalizedLiteral, normalizedValue);
            return normalizedLiteral;
        } catch (Exception e) {
            // If we can't normalize a term, note that, and quit
            if (log.isTraceEnabled()) {
                log.trace("Failed to normalized " + node.getLiteral() + " with " + normalizer != null ? normalizer.getClass().getName() : null + " normalizer");
            }

            this.normalizationFailed = true;
            return node;
        }
    }
}
