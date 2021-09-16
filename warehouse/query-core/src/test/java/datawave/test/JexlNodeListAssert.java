package datawave.test;

import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.commons.jexl2.parser.JexlNode;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ListAssert;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.util.Lists.newArrayList;

/**
 * A {@link AbstractListAssert} implementation that supports performing AssertJ assertions on a {@link List} of {@link JexlNode} instances. Equality between
 * nodes will be determined using {@link DeepJexlNodeComparator}.
 */
public class JexlNodeListAssert extends AbstractListAssert<JexlNodeListAssert,List<? extends JexlNode>,JexlNode,JexlNodeAssert> {
    
    protected JexlNodeListAssert(List<? extends JexlNode> jexlNodes) {
        super(jexlNodes, JexlNodeListAssert.class);
        // noinspection ResultOfMethodCallIgnored
        usingElementComparator(new DeepJexlNodeComparator());
    }
    
    @Override
    protected JexlNodeAssert toAssert(JexlNode value, String description) {
        return new JexlNodeAssert(value).as(description);
    }
    
    @Override
    protected JexlNodeListAssert newAbstractIterableAssert(Iterable<? extends JexlNode> iterable) {
        return new JexlNodeListAssert(newArrayList(iterable));
    }
    
    /**
     * Transform each node into their respective query string via {@link JexlStringBuildingVisitor#buildQuery(JexlNode)} and return a new {@link ListAssert}
     * that will perform assertions on an {@link ArrayList} of the query strings.
     * <p>
     * Example:
     * 
     * <pre>
     * <code class='java'>
     * List&lt;JexlNode&gt; nodes = new ArrayList&lt;&gt;();
     * nodes.add(JexlASTHelper.parseJexlQuery("FOO == 'bar'"));
     * 
     * JexlNodeListAssert nodeAssert = new JexlNodeListAssert(nodes);
     * // Assertion will pass.
     * assertThat(nodeAssert).asStrings().contains("FOO == 'bar'");</code>
     * </pre>
     * 
     * @return a new {@link ListAssert} of the query strings
     */
    public ListAssert<String> asStrings() {
        return asStrings(JexlStringBuildingVisitor::buildQuery);
    }
    
    /**
     * Transform each node into a string using the provided function and return a new {@link ListAssert} that will perform assertions on an {@link ArrayList} of
     * the strings. This method should be used when {@link JexlStringBuildingVisitor#buildQuery(JexlNode)} is not the appropriate transformation function.
     * Example where the function {@link JexlStringBuildingVisitor#buildQueryWithoutParse(JexlNode)} is used instead:
     * 
     * <pre>
     * <code class='java'>
     * List&lt;JexlNode&gt; nodes = new ArrayList&lt;&gt;();
     * nodes.add(JexlASTHelper.parseJexlQuery("FOO == 'bar'"));
     * 
     * JexlNodeListAssert nodeAssert = new JexlNodeListAssert(nodes);
     * // Assertion will pass.
     * assertThat(nodeAssert).asStrings(JexlStringBuildingVisitor::buildQueryWithoutParse).contains("FOO == 'bar'");</code>
     * </pre>
     * 
     * @param function
     *            the function to use to convert the nodes to strings
     * @return a new {@link ListAssert} of the strings
     */
    public ListAssert<String> asStrings(Function<JexlNode,String> function) {
        if (actual == null) {
            return new ListAssert<>((List<? extends String>) null);
        } else {
            List<String> list = actual.stream().map(function).collect(Collectors.toList());
            return new ListAssert<>(list);
        }
    }
}
