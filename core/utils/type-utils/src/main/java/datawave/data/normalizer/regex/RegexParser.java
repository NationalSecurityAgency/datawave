package datawave.data.normalizer.regex;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * This parser will create a {@link Node} tree parsed from a regex pattern. This parser will be used for normalizing numeric regex patterns, and as such is not
 * intended to be a fully comprehensive regex parser. Some native regex characters may be restricted.
 */
public class RegexParser {
    
    /**
     * Parses the given regex and returns a {@link ExpressionNode} tree representing the parsed regex. If the string is null, null will be returned.
     * 
     * @param regex
     *            the regex to parse
     * @return the {@link Node} tree
     */
    public static ExpressionNode parse(String regex) {
        if (regex == null) {
            return null;
        }
        Node node = parseAlternations(regex);
        // Ensure the root node is always an expression node.
        return node instanceof ExpressionNode ? (ExpressionNode) node : createExpressionWithChild(node);
    }
    
    /**
     * Parses a regex expression from the given string that may contain alternations. Depending on the expression, one of the following will be returned:
     * <ul>
     * <li>An {@link EmptyNode} will be returned if a blank string is given.</li>
     * <li>If the expression contains top-level alternations, an {@link ExpressionNode} with an {@link AlternationNode} as its child with its child alternating
     * expressions will be returned.</li>
     * <li>If the expression does not contain any top-level alternations, an {@link ExpressionNode} with the parsed expression as its children will be
     * returned.</li>
     * </ul>
     * 
     * @param string
     *            the string
     * @return the parsed node
     */
    private static Node parseAlternations(String string) {
        // If the string is blank, return an EmptyNode.
        if (StringUtils.isBlank(string)) {
            return new EmptyNode();
        }
        
        List<String> expressions = RegexUtils.splitOnAlternations(string);
        Node node;
        if (expressions.size() > 1) {
            // If we have more than one expression, we must make the parsed expressions children of an alternation node.
            node = new AlternationNode();
            for (String segment : expressions) {
                Node child = parseAlternations(segment);
                if (child != null) {
                    node.addChild(child);
                }
            }
        } else if (expressions.size() == 1) {
            node = parseExpression(expressions.get(0));
        } else {
            return null;
        }
        // If the parsed node is not an AlternationNode, GroupNode, or ExpressionNode, wrap it in an ExpressionNode.
        return requiresWrap(node) ? createExpressionWithChild(node) : node;
    }
    
    /**
     * Parses a subset of a regex expression that does not contain any top-level alternations, i.e. pipes.
     * 
     * @param string
     *            the regex to parse
     * @return the parsed node
     */
    private static Node parseExpression(String string) {
        
        // If the string is blank, return an EmptyNode.
        if (StringUtils.isBlank(string)) {
            return new EmptyNode();
        }
        
        List<Node> nodes = new ArrayList<>();
        RegexReader reader = new RegexReader(string);
        while (reader.hasNext()) {
            reader.captureNext();
            RegexReader.ExpressionType type = reader.capturedType();
            String content = reader.capturedExpression();
            nodes.add(createNode(type, content));
        }
        
        // If we have a single child parsed from the expression, wrap it in an expression node if it is not already a wrapper node. Otherwise, return the child.
        if (nodes.size() == 1) {
            Node child = nodes.get(0);
            return requiresWrap(child) ? createExpressionWithChild(child) : child;
        } else {
            // Wrap the children in an expression node.
            ExpressionNode expressionNode = new ExpressionNode();
            expressionNode.setChildren(nodes);
            return expressionNode;
        }
    }
    
    /**
     * Return a new {@link ExpressionNode} with the given node as its child.
     * 
     * @param child
     *            the child
     * @return the new node
     */
    private static ExpressionNode createExpressionWithChild(Node child) {
        ExpressionNode node = new ExpressionNode();
        node.addChild(child);
        return node;
    }
    
    /**
     * Return whether the given node should be wrapped in an {@link ExpressionNode}. A node should not be wrapped if it is an instance of one of the following:
     * <ul>
     * <li>{@link ExpressionNode}</li>
     * <li>{@link GroupNode}</li>
     * <li>{@link AlternationNode}</li>
     * </ul>
     * 
     * @param node
     *            the node
     * @return true if the given node is a wrapper type, or false otherwise.
     */
    private static boolean requiresWrap(Node node) {
        return node != null && !(node instanceof ExpressionNode || node instanceof AlternationNode || node instanceof GroupNode);
    }
    
    /**
     * Return a new node of the specified type with the given content if applicable.
     *
     * @param type
     *            the node type to create
     * @param content
     *            the content
     * @return the new node
     */
    private static Node createNode(RegexReader.ExpressionType type, String content) {
        switch (type) {
            case ANCHOR_START:
                return new StartAnchorNode();
            case ANCHOR_END:
                return new EndAnchorNode();
            case ESCAPED_CHAR:
                return createNodeFromEscapedChar(content);
            case ANY_CHAR:
                return new AnyCharNode();
            case ZERO_OR_MORE:
                return new ZeroOrMoreNode();
            case ONE_OR_MORE:
                return new OneOrMoreNode();
            case QUESTION_MARK:
                return new QuestionMarkNode();
            case SINGLE_CHAR:
                return new SingleCharNode(content.charAt(0));
            case REPETITION:
                return createRepetitionNode(content);
            case CHAR_CLASS:
                return createCharClassNode(content);
            case GROUP:
                return createGroupNode(content);
            default:
                throw new IllegalArgumentException("Unable to create new node of type " + type);
        }
    }
    
    /**
     * Return a new {@link Node} from the given escaped character. In the case of {@code \d}, a new {@link DigitCharClassNode} will be returned. Otherwise, a
     * new {@link EscapedSingleCharNode} with the character will be returned.
     * 
     * @param content
     *            the content
     * @return the new node
     */
    private static Node createNodeFromEscapedChar(String content) {
        char character = content.charAt(1);
        if (character == RegexConstants.LOWERCASE_D) {
            return new DigitCharClassNode();
        }
        return new EscapedSingleCharNode(character);
    }
    
    /**
     * Return a new {@link RepetitionNode} parsed from the given expression. It is expected that the given content is an interval expression in the form
     * {@code {x}}, {@code {x,y}}, {@code {x,}}, or {@code {,y}}.
     *
     * @param expression
     *            the interval expression
     * @return the node
     */
    private static RepetitionNode createRepetitionNode(String expression) {
        RepetitionNode node = new RepetitionNode();
        int commaIndex = expression.indexOf(RegexConstants.COMMA);
        if (commaIndex == -1) {
            // If no comma is present, the interval expression is in the form {x}. Remove the curly braces and parse the number from x.
            node.addChild(new IntegerNode(Integer.parseInt(trimFirstAndLastChar(expression))));
        } else {
            // If a comma is present, the interval expression is in the form {x,y} or {x,}. Remove the curly braces and parse the range from x and y.
            int start = Integer.parseInt(expression.substring(1, commaIndex));
            Integer end = commaIndex == (expression.length() - 2) ? null : Integer.parseInt(expression.substring((commaIndex + 1), (expression.length() - 1)));
            node.addChild(new IntegerRangeNode(start, end));
        }
        return node;
    }
    
    /**
     * Return a new {@link CharClassNode} parsed from the given expression. Parsing negated character classes is supported. The character class may only contain
     * the following: digits, a period, a hyphen, a numerical range.
     *
     * @param expression
     *            the character class expression
     * @return the node
     */
    private static CharClassNode createCharClassNode(String expression) {
        CharClassNode node = new CharClassNode();
        char[] chars = expression.toCharArray();
        char next;
        for (int pos = 1; pos < (chars.length - 1); pos++) {
            char current = chars[pos];
            switch (current) {
                case RegexConstants.HYPHEN:
                    // We found a hyphen at the start or end of the character class, e.g. [-123] or [123-]. Hyphens do not need to be escaped in these cases.
                    node.addChild(new SingleCharNode(current));
                    break;
                case RegexConstants.BACKSLASH:
                    // We found an escaped character.
                    next = chars[(pos) + 1];
                    node.addChild(new EscapedSingleCharNode(next));
                    pos++;
                    break;
                case RegexConstants.CARET:
                    // If the caret is the first character in the class, we have a negated character class, e.g. [^123].
                    if (pos == 1) {
                        node.negate();
                    } else {
                        // Otherwise add it as a single character.
                        node.addChild(new SingleCharNode(current));
                    }
                    break;
                default:
                    // Check if we have a non-trailing hyphen that indicates a defined character range.
                    next = chars[(pos + 1)];
                    if (next == RegexConstants.HYPHEN) {
                        char charAfterNext = chars[(pos) + 2];
                        // If the next character is not a closing bracket, we have a character range. Otherwise, the hyphen will need to be captured as its own
                        // single character in an earlier switch case,
                        if (charAfterNext != RegexConstants.RIGHT_BRACKET) {
                            node.addChild(new CharRangeNode(current, charAfterNext));
                            // Move to the next character after the range.
                            pos = pos + 2;
                        }
                    } else {
                        // Otherwise, add the current character as a single character.
                        node.addChild(new SingleCharNode(current));
                    }
                    break;
            }
        }
        return node;
    }
    
    /**
     * Return a new {@link GroupNode} parsed from the given expression.
     *
     * @param expression
     *            the group expression
     * @return the node
     */
    private static GroupNode createGroupNode(String expression) {
        String subExpression = trimFirstAndLastChar(expression);
        GroupNode groupNode = new GroupNode();
        Node node = parseAlternations(subExpression);
        if (node != null) {
            groupNode.addChild(node);
        }
        return groupNode;
    }
    
    /**
     * Return the given string with the first and last character trimmed. If the string has a length less than 3, an empty string will be returned.
     *
     * @param str
     *            the string
     * @return the trimmed string
     */
    private static String trimFirstAndLastChar(String str) {
        if (str.length() < 3) {
            return "";
        } else {
            return str.substring(1, (str.length() - 1));
        }
    }
    
    /**
     * Do not allow this class to be instantiated.
     */
    private RegexParser() {
        throw new UnsupportedOperationException();
    }
}
