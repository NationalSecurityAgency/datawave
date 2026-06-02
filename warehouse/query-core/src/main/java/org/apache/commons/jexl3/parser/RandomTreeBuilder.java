package org.apache.commons.jexl3.parser;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

import com.google.common.collect.Lists;

import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Counts the total number of nodes for each node type present in a query tree.
 */
public class RandomTreeBuilder {
    private static final Logger log = Logger.getLogger(RandomTreeBuilder.class);

    static final Random random = new Random();
    static final List<Class<? extends JexlNode>> types;

    static {
        types = getSubClasses(JexlNode.class);
        // remove the times for which there is not a parser visitor method
        Iterator<Class<? extends JexlNode>> it = types.iterator();
        while (it.hasNext()) {
            Class<? extends JexlNode> c = it.next();
            try {
                ParserVisitor.class.getDeclaredMethod("visit", c, Object.class);
            } catch (Exception e) {
                it.remove();
            }
        }
    }

    private static <T> List<T> asList(Set<T> set) {
        return Lists.newArrayList(set);
    }

    private static <T> List<Class<? extends T>> getSubClasses(Class<T> parent) {
        return getSubClasses(parent, parent.getPackageName());
    }

    private static <T> List<Class<? extends T>> getSubClasses(Class<T> parent, String packageName) {
        ClassPathScanningCandidateComponentProvider p = new ClassPathScanningCandidateComponentProvider(false);
        p.addIncludeFilter(new AssignableTypeFilter(parent));
        Set<BeanDefinition> candidates = p.findCandidateComponents(packageName);
        return candidates.stream().map(b -> {
            try {
                return (Class<? extends T>) Class.forName(b.getBeanClassName());
            } catch (Exception e) {
                return null;
            }
        }).filter(b -> b != null).collect(Collectors.toList());
    }

    static class Counts {
        int total;
        Map<String,Integer> countMap;

        public void increment(String c) {
            increment(c, 1);
        }

        public void increment(String c, int inc) {
            total += inc;
            countMap.compute(c, (key, val) -> (val == null) ? inc : val + inc);
        }
    }

    public static ASTJexlScript build(Map<String,Integer> countMap, int size) {
        Counts counts = new Counts();
        counts.total = 0;
        counts.countMap = countMap;

        // start with one script node and then randomly create stuff under it
        ASTJexlScript script = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        counts.increment(ASTJexlScript.class.getName());
        if (size > 1) {
            addRandomChildren(script, script, counts, size);
        }
        return script;
    }

    public static void addRandomChildren(ASTJexlScript tree, JexlNode node, Counts counts, int size) {
        if (node.jjtGetNumChildren() == 0 && size > counts.total) {
            int children = Math.min(random.nextInt(size - counts.total), 4) + 1;
            for (int i = 0; i < children; i++) {
                if (counts.total < size) {
                    JexlNode child = createNodeOrMarker(counts, size);
                    node.jjtAddChild(child, i);
                    child.jjtSetParent(node);
                }
            }
            children = node.jjtGetNumChildren();
            if (size > counts.total) {
                int childrenPerNode = (size - counts.total) / children;
                int remainder = (size - counts.total) % children;
                for (int i = 0; i < children; i++) {
                    addRandomChildren(tree, node.jjtGetChild(i), counts, counts.total + (i == 0 ? childrenPerNode + remainder : childrenPerNode));
                }
            }
        }
    }

    public static JexlNode createNodeOrMarker(Counts counts, int size) {
        // do not create a marker if we don't have room for at least 8 additional nodes
        if ((size - counts.total) < 8 || random.nextBoolean()) {
            return createNode(counts);
        } else {
            return createMarker(counts);
        }
    }

    public static JexlNode createMarker(Counts counts) {
        QueryPropertyMarker.MarkerType type = QueryPropertyMarker.MarkerType.values()[random.nextInt(QueryPropertyMarker.MarkerType.values().length)];
        Counts markerCounts = new Counts();
        markerCounts.total = 0;
        markerCounts.countMap = new HashMap<>();
        JexlNode source = createNode(markerCounts);
        JexlNode marker = QueryPropertyMarker.create(source, type);
        if (marker != source) {
            markerCounts.increment(ASTReferenceExpression.class.getName());
            markerCounts.increment(ASTAssignment.class.getName());
            markerCounts.increment(ASTIdentifier.class.getName());
            markerCounts.increment(ASTTrueNode.class.getName());

            if (!(source instanceof ASTReferenceExpression)) {
                markerCounts.increment(ASTReferenceExpression.class.getName());
            }

            markerCounts.increment(type.getLabel());

            if (!(source.jjtGetParent() instanceof ASTReferenceExpression)) {
                markerCounts.increment(ASTReferenceExpression.class.getName());
            }
        }
        markerCounts.countMap.forEach((k, v) -> counts.increment(k, v));

        return marker;
    }

    public static JexlNode createNode(Counts counts) {
        JexlNode n = null;
        while (n == null) {
            Class<? extends JexlNode> nodeClass = types.get(random.nextInt(types.size()));
            try {
                Constructor<? extends JexlNode> c = nodeClass.getConstructor(int.class);
                n = c.newInstance(0);
                if (n instanceof ASTNumberLiteral) {
                    JexlNodes.setLiteral((ASTNumberLiteral) n, 1);
                } else if (n instanceof ASTRegexLiteral) {
                    JexlNodes.setLiteral((ASTRegexLiteral) n, ".*");
                } else if (n instanceof ASTJxltLiteral) {
                    JexlNodes.setLiteral((ASTJxltLiteral) n, "?");
                } else if (n instanceof ASTStringLiteral) {
                    JexlNodes.setLiteral((ASTStringLiteral) n, "hi");
                } else if (n instanceof ASTIdentifier) {
                    JexlNodes.setIdentifier((ASTIdentifier) n, "x");
                } else if (n instanceof ASTAnnotation) {
                    JexlNodes.setAnnotation((ASTAnnotation) n, "*");
                } else if (n instanceof ASTIdentifierAccess) {
                    JexlNodes.setIdentifierAccess((ASTIdentifierAccess) n, "1");
                } else if (n instanceof ASTQualifiedIdentifier) {
                    JexlNodes.setQualifiedIdentifier((ASTQualifiedIdentifier) n, "q");
                }
                counts.increment(nodeClass.getName());
            } catch (Exception e) {
                // remove this type from the types to avoid doing this again
                types.remove(nodeClass);
            }
        }
        return n;
    }

}
