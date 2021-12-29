package datawave.test;

import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.commons.jexl2.parser.JexlNode;
import org.assertj.core.api.AbstractIterableAssert;
import org.assertj.core.api.IterableAssert;
import org.assertj.core.api.ListAssert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link AbstractIterableAssert} implementation that supports performing AssertJ assertions on an {@link Iterable} of {@link JexlNode} instances. Equality
 * between nodes will be determined using {@link DeepJexlNodeComparator}.
 */
public class JexlNodeIterableAssert extends AbstractIterableAssert<JexlNodeIterableAssert,Iterable<? extends JexlNode>,JexlNode,JexlNodeAssert> {
    
    protected JexlNodeIterableAssert(Iterable<? extends JexlNode> jexlNodes) {
        super(jexlNodes, JexlNodeIterableAssert.class);
        // noinspection ResultOfMethodCallIgnored
        usingElementComparator(new DeepJexlNodeComparator());
    }
    
    @Override
    protected JexlNodeAssert toAssert(JexlNode value, String description) {
        return new JexlNodeAssert(value).as(description);
    }
    
    @Override
    protected JexlNodeIterableAssert newAbstractIterableAssert(Iterable<? extends JexlNode> iterable) {
        return new JexlNodeIterableAssert(iterable);
    }
    
    /**
     * Transform each node into their respective query string via {@link JexlStringBuildingVisitor#buildQuery(JexlNode)} and return a new {@link ListAssert}
     * that will perform assertions on an {@link ArrayList} of the query strings.
     * <p>
     * Example:
     * 
     * <pre>
     * <code class='java'>
     * Set&lt;JexlNode&gt; nodes = new HashSet&lt;&gt;();
     * nodes.add(JexlASTHelper.parseJexlQuery("FOO == 'bar'"));
     * 
     * JexlNodeIterableAssert nodeAssert = new JexlNodeIterableAssert(nodes);
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
     * <p>
     * Example where the function {@link JexlStringBuildingVisitor#buildQueryWithoutParse(JexlNode)} is used instead:
     * 
     * <pre>
     * <code class='java'>
     * Set&lt;JexlNode&gt; nodes = new HashSet&lt;&gt;();
     * nodes.add(JexlASTHelper.parseJexlQuery("FOO == 'bar'"));
     * 
     * JexlNodeIterableAssert nodeAssert = new JexlNodeIterableAssert(nodes);
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
            List<String> list = getNodeStrings(ArrayList::new, function);
            return new ListAssert<>(list);
        }
    }
    
    /**
     * Transform each node into their respective query string via {@link JexlStringBuildingVisitor#buildQuery(JexlNode)} and return a new {@link IterableAssert}
     * that will perform assertions on a collection of the supplied constructor type that contains the query strings. This method should be used instead of
     * {@link #asStrings()} when the default {@link ArrayList} is not desired.
     * <p>
     * Example where the query strings will be stored in a new {@link java.util.HashSet}.
     * 
     * <pre>
     * <code class='java'>
     * Set&lt;JexlNode&gt; nodes = new HashSet&lt;&gt;();
     * nodes.add(JexlASTHelper.parseJexlQuery("FOO == 'bar'"));
     * 
     * JexlNodeIterableAssert nodeAssert = new JexlNodeIterableAssert(nodes);
     * // Assertion will pass.
     * assertThat(nodeAssert).asStrings(HashSet::new).contains("FOO == 'bar'");</code>
     * </pre>
     *
     * @return a new {@link ListAssert} of the query strings
     */
    public IterableAssert<String> asStrings(Supplier<? extends Collection<String>> constructor) {
        return asStrings(constructor, JexlStringBuildingVisitor::buildQuery);
    }
    
    /**
     * Transform each node into string using the provided function and return a new {@link IterableAssert} that will perform assertions on a collection of the
     * supplied constructor type that contains the strings. This method should be used when the default {@link ArrayList} and
     * {@link JexlStringBuildingVisitor#buildQuery(JexlNode)} function needs to be overridden.
     * <p>
     * Example where the query strings will be stored in a new {@link java.util.HashSet} and transformed using
     * {@link JexlStringBuildingVisitor#buildQueryWithoutParse(JexlNode)}.
     * 
     * <pre>
     * <code class='java'>
     * Set&lt;JexlNode&gt; nodes = new HashSet&lt;&gt;();
     * nodes.add(JexlASTHelper.parseJexlQuery("FOO == 'bar'"));
     * 
     * JexlNodeIterableAssert nodeAssert = new JexlNodeIterableAssert(nodes);
     * // Assertion will pass.
     * assertThat(nodeAssert).asStrings(HashSet::new, JexlStringBuildingVisitor::buildQueryWithoutParse).contains("FOO == 'bar'");</code>
     * </pre>
     *
     * @return a new {@link ListAssert} of the query strings
     */
    public IterableAssert<String> asStrings(Supplier<? extends Collection<String>> constructor, Function<JexlNode,String> function) {
        if (actual == null) {
            return new IterableAssert<>(null);
        } else {
            Collection<String> collection = getNodeStrings(constructor, function);
            return new IterableAssert<>(collection);
        }
    }
    
    // Return a collection of the specified type with the node strings as transformed by the given function.
    private <T extends Collection<String>> T getNodeStrings(Supplier<T> constructor, Function<JexlNode,String> function) {
        T collection = constructor.get();
        for (JexlNode node : actual) {
            collection.add(function.apply(node));
        }
        return collection;
    }
    
}
