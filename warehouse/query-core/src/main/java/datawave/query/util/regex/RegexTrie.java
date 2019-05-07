package datawave.query.util.regex;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class can be used to turn a large list of strings into a regex. This may be used to handle queries that arrive with lists so large that the term limit
 * is reached.
 */
public class RegexTrie {
    final Node root = new Node();
    
    class Node {
        final HashMap<Character,Node> next = new HashMap<>();
        boolean terminator = false;
    }
    
    public RegexTrie() {}
    
    public RegexTrie(List<String> strings) {
        addAll(strings);
    }
    
    public RegexTrie(String... strings) {
        addAll(Arrays.asList(strings));
    }
    
    /**
     * Add a set of strings into the trie
     * 
     * @param strings
     *            A list of strings. May contain the empty string.
     */
    public void addAll(List<String> strings) {
        for (String string : strings) {
            add(string);
        }
    }
    
    /**
     * Add a string into the trie
     * 
     * @param string
     *            A string. May be the empty string.
     */
    public void add(String string) {
        
        // starting with the root node
        Node current = root;
        
        // for each character in the string
        for (int i = 0; i < string.length(); i++) {
            current = current.next.computeIfAbsent(string.charAt(i), n -> new Node());
        }
        
        // mark the last node as a termination point
        current.terminator = true;
    }
    
    /**
     * Determine if this trie contains a string
     * 
     * @param string
     *            A string
     */
    public boolean contains(final String string) {
        // starting with the root node
        Node current = root;
        final int length = string.length();
        
        // for each character in the string
        for (int i = 0; i < length; i++) {
            // get the node for this character
            current = current.next.get(string.charAt(i));
            // if no node, then no match
            if (current == null) {
                return false;
            }
        }
        
        // if we got here, then we matched the entire string.
        // We have a true match if this node was marked as a termination point
        return current.terminator;
    }
    
    /**
     * Convert this trie into a Java regex
     * 
     * @return a Java regex
     */
    public String toRegex() {
        StringBuilder regex = new StringBuilder();
        // starting with the root, build the regex
        toRegex(root, regex);
        // return the resulting string
        return regex.toString();
    }
    
    /**
     * A recursive method to turn a trie tree into a regex
     * 
     * @param current
     *            The root of the tree
     * @param regex
     *            The regex string builder
     */
    private void toRegex(Node current, StringBuilder regex) {
        // if the node does not have any following characters, then we are done
        if (!current.next.isEmpty()) {
            // do we have multiple choices at this point:
            // if this is a termination point or we have more than one following character
            boolean multipleChoices = (current.terminator || current.next.size() > 1);
            // if we have multiple choices then start a group
            if (multipleChoices) {
                regex.append('(');
            }
            // if this is a termination point, then one of the choices is the empty string
            boolean first = !current.terminator;
            // for each following character
            for (Map.Entry<Character,Node> entry : current.next.entrySet()) {
                // if not the first choice, then append separate the choices
                if (!first) {
                    regex.append('|');
                } else {
                    first = false;
                }
                // escape the character if needed
                if (escape(entry.getKey())) {
                    regex.append('\\');
                }
                // append the character
                regex.append(entry.getKey());
                // and recursively follow the sub-tree
                toRegex(entry.getValue(), regex);
            }
            // if we had multiple choices then end the group
            if (multipleChoices) {
                regex.append(')');
            }
        }
    }
    
    private static final char[] CHARS_TO_ESCAPE = new char[] {'\\', '.', '[', ']', '{', '}', '(', ')', '<', '>', '*', '+', '-', '=', '!', '?', '^', '$', '|'};
    
    /**
     * Does this character require escaping
     * 
     * @param c
     * @return true if escaping is required
     */
    private boolean escape(char c) {
        for (char ec : CHARS_TO_ESCAPE) {
            if (c == ec) {
                return true;
            }
        }
        return false;
    }
    
}
