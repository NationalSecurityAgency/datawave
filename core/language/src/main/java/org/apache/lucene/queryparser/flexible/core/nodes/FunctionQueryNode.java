package org.apache.lucene.queryparser.flexible.core.nodes;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax.Type;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import datawave.core.query.language.parser.lucene.ParseException;

/**
 * A {@link FieldQueryNode} represents a element that contains field/text tuple
 */
public class FunctionQueryNode extends QueryNodeImpl {

    private static final long serialVersionUID = 3634521145130758265L;

    /**
     * The term's text.
     */
    protected CharSequence text;

    /**
     * The term's begin position.
     */
    protected int begin;

    /**
     * The term's end position.
     */
    protected int end;

    /**
     * The term's position increment.
     */
    protected int positionIncrement;

    protected String function;

    protected List<String> parameterList = new ArrayList<>();

    /**
     * @param begin
     *            - position in the query string
     * @param end
     *            - position in the query string
     * @param text
     *            text string
     * @throws ParseException
     *             for issues parsing
     */
    public FunctionQueryNode(CharSequence text, int begin, int end) throws ParseException {
        String s = text.toString();
        int openParen = s.indexOf("(");
        int closeParen = s.lastIndexOf(")");
        this.function = s.substring(1, openParen);

        Character endQuote = null;
        Character beginQuote = null;
        boolean paramStarted = false;
        UnescapedCharSequence chars = (UnescapedCharSequence) text;
        int currArgStart = openParen + 1;
        for (int x = openParen + 1; x < closeParen; x++) {

            char c = chars.charAt(x);
            if (paramStarted == false) {
                if (Character.isWhitespace(c) == false) {
                    if ((c == '"' || c == '\'') && chars.wasEscaped(x) == false) {
                        beginQuote = c;
                    } else {
                        beginQuote = null;
                        endQuote = null;
                    }
                    paramStarted = true;
                }
            } else {
                // quoted parameter has been closed but not added to parameterList yet
                // only whitespace, a comma, or an end paren should be next
                // anything else should cause a parse error
                if (beginQuote == null && endQuote != null) {
                    if (Character.isWhitespace(c) == false && c != ')' && c != ',') {
                        throw new RuntimeException("Unexpected characters '" + chars.subSequence(x, closeParen) + "' following quoted parameter in [" + text
                                        + "], expecting ',' or ')'");
                    }
                }

                if (beginQuote != null && Character.valueOf(c).equals(beginQuote) && chars.wasEscaped(x) == false) {
                    endQuote = beginQuote;
                    beginQuote = null;
                }
            }

            String seq = null;
            if (c == ',' && chars.wasEscaped(x) == false && beginQuote == null) {
                UnescapedCharSequence unescapedSeq = (UnescapedCharSequence) chars.subSequence(currArgStart, x);
                if (endQuote != null) {
                    if (endQuote == '\'') {
                        seq = toStringEscaped(unescapedSeq, new char[] {'"', '(', ')', ',', '\\'}).toString().trim();
                    } else {
                        seq = toStringEscaped(unescapedSeq, new char[] {'\'', '(', ')', ',', '\\'}).toString().trim();
                    }
                    int start = seq.indexOf(endQuote);
                    int stop = seq.lastIndexOf(endQuote);
                    seq = seq.substring(start + 1, stop);
                } else {
                    seq = toStringEscaped(unescapedSeq, new char[] {'"', '\''}).toString().trim();
                }
                this.parameterList.add(seq);
                paramStarted = false;
                currArgStart = x + 1;
            } else if (x + 1 == closeParen) {
                UnescapedCharSequence unescapedSeq = (UnescapedCharSequence) chars.subSequence(currArgStart, closeParen);
                if (endQuote != null) {
                    if (beginQuote != null) {
                        throw new ParseException(new MessageImpl("Reached end of parameter list " + text + " looking for matching " + beginQuote));
                    }

                    if (endQuote == '\'') {
                        seq = toStringEscaped(unescapedSeq, new char[] {'"', '(', ')', ',', '\\'}).toString().trim();
                    } else {
                        seq = toStringEscaped(unescapedSeq, new char[] {'\'', '(', ')', ',', '\\'}).toString().trim();
                    }

                    int start = seq.indexOf(endQuote);
                    int stop = seq.lastIndexOf(endQuote);
                    seq = seq.substring(start + 1, stop);
                } else {
                    seq = toStringEscaped(unescapedSeq, new char[] {'"', '\''}).toString().trim();
                }
                this.parameterList.add(seq);
                paramStarted = false;
                currArgStart = x + 1;
            }
        }

        this.begin = begin;
        this.end = end;
        this.setLeaf(true);

    }

    private String toStringEscaped(UnescapedCharSequence seq, char[] enabledChars) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < seq.length(); i++) {
            for (char character : enabledChars) {
                if (seq.charAt(i) == character && seq.wasEscaped(i)) {
                    result.append('\\');
                    break;
                }
            }

            result.append(seq.charAt(i));
        }
        return result.toString();
    }

    public String getFunction() {
        return function;
    }

    public List<String> getParameterList() {
        return parameterList;
    }

    protected CharSequence getTermEscaped(EscapeQuerySyntax escaper) {
        return escaper.escape(this.text, Locale.getDefault(), Type.NORMAL);
    }

    protected CharSequence getTermEscapeQuoted(EscapeQuerySyntax escaper) {
        return escaper.escape(this.text, Locale.getDefault(), Type.STRING);
    }

    public CharSequence toQueryString(EscapeQuerySyntax escaper) {
        return getTermEscaped(escaper);
    }

    @Override
    public String toString() {
        return "<function start='" + this.begin + "' end='" + this.end + "' text='" + this.text + "'/>";
    }

    public int getBegin() {
        return this.begin;
    }

    public void setBegin(int begin) {
        this.begin = begin;
    }

    public int getEnd() {
        return this.end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public int getPositionIncrement() {
        return this.positionIncrement;
    }

    public void setPositionIncrement(int pi) {
        this.positionIncrement = pi;
    }

    @Override
    public FunctionQueryNode cloneTree() throws CloneNotSupportedException {
        FunctionQueryNode fqn = (FunctionQueryNode) super.cloneTree();
        fqn.begin = this.begin;
        fqn.end = this.end;
        fqn.positionIncrement = this.positionIncrement;

        return fqn;
    }

}
