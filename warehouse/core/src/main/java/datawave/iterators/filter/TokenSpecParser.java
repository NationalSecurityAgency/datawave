package datawave.iterators.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import datawave.iterators.filter.ageoff.AgeOffPeriod;

public abstract class TokenSpecParser<B extends TokenSpecParser> {
    /**
     * Add a new token with its TTL to the the structure.
     */
    public abstract B addToken(byte[] token, long ttl);

    /**
     * Parse additional token configurations from a string.
     */
    public B parse(String configuration) {
        ParserState parser = new ParserState(configuration);
        parser.parseTo(this);
        return (B) this;
    }

    /**
     * Minimal grammar for parsing tokens specs:
     *
     * <pre>
     * tokens    &lt;- &lt;empty&gt;
     *           &lt;- &lt;tokens&gt;&lt;separator&gt;&lt;tokenspec&gt;
     * tokenspec &lt;- "strliteral"
     *           &lt;- "strliteral" : &lt;num&gt;&lt;unit&gt;
     *           &lt;- label strliteral2
     *           &lt;- label strliteral2=&lt;num&gt;&lt;unit&gt;
     * separator &lt;- ,
     *           &lt;- \n
     *           &lt;- &lt;space&gt;
     *           &lt;- &lt;separator&gt;&lt;separator&gt;
     * </pre>
     */
    protected static class ParserState {

        private enum ParseTokenType {
            //@formatter:off
            SEPARATION("[\\s\\n,]+"),
            STRLITERAL("\"(?:[^\\\"]|\\.)*\""), // "strliteral"
            LABELED_STRLITERAL("[\\w]+[ \\t]+[\\w]+"), // label strliteral2
            COLON(":"),
            EQUALS("="),
            NUMBER("[0-9]+"),
            UNIT(AgeOffTtlUnits.MILLISECONDS + "|" +
                    AgeOffTtlUnits.DAYS + "|" +
                    AgeOffTtlUnits.HOURS + "|" +
                    AgeOffTtlUnits.MINUTES + "|" +
                    AgeOffTtlUnits.SECONDS);
            //@formatter:on

            private final Pattern matchPattern;

            ParseTokenType(String matchExpr) {
                this.matchPattern = Pattern.compile(matchExpr);
            }
        }

        private static class ParseToken {

            public ParseTokenType type;
            public String content;
            public int offset;

            public ParseToken(ParseTokenType type, String content, int offset) {
                this.type = type;
                this.content = content;
                this.offset = offset;
            }
        }

        private final String input;
        private final List<ParseToken> parseTokens;
        private int nextTokenPos;

        protected ParserState(String input) {
            this.input = input;
            this.parseTokens = tokenize(input);
            nextTokenPos = 0;
        }

        private IllegalArgumentException error(String message, int posn) {
            return error(message, posn, null);
        }

        private IllegalArgumentException error(String message, int posn, Throwable cause) {
            return new IllegalArgumentException(message + " near ..." + input.substring(posn, Math.min(input.length(), posn + 20)) + "...", cause);
        }

        private List<ParseToken> tokenize(String input) {
            List<ParseToken> result = new ArrayList<>();
            int curPos = 0;
            while (curPos < input.length()) {
                boolean foundMatch = false;
                nextToken: for (ParseTokenType type : ParseTokenType.values()) {
                    // To avoid quadratic parsing time, initially assume no token is more than 50 characters long.
                    int maxTokenLength = 50;
                    while (true) {
                        Matcher m = type.matchPattern.matcher(input.substring(curPos, Math.min(input.length(), curPos + maxTokenLength)));
                        if (m.lookingAt()) {
                            if (m.end() == maxTokenLength) {
                                // oops, seems we found a token longer than expected. Double expected and try again.
                                maxTokenLength += maxTokenLength;
                                continue;
                            }
                            foundMatch = true;
                            if (type != ParseTokenType.SEPARATION) {
                                result.add(new ParseToken(type, input.substring(curPos, curPos + m.end()), curPos));
                            }
                            curPos += m.end();
                            break nextToken;
                        } else {
                            break;
                        }
                    }
                }
                if (!foundMatch) {
                    throw error("Failed to tokenize", curPos);
                }
            }
            return result;
        }

        /**
         * Return the next token without advancing.
         */
        protected ParseToken peek() {
            if (nextTokenPos >= parseTokens.size()) {
                return null;
            }
            return parseTokens.get(nextTokenPos);
        }

        /**
         * Consume the next token, assuming it's of the specified type, and return its content.
         */
        protected String expect(ParseTokenType type) {
            ParseToken next = peek();
            if (next == null || next.type != type) {
                throw error("Expected a " + type, next.offset);
            }
            nextTokenPos++;
            return next.content;
        }

        /**
         * Parse the entire input and add it to the TtlTrieBuilder.
         */
        protected void parseTo(TokenSpecParser builder) {
            ParseToken initialToken;
            while ((initialToken = peek()) != null) {
                String tokenStr = parseStrliteral();

                long ttl = -1;
                if (peek() != null && peek().type == ParseTokenType.COLON) {
                    ttl = parseTtl(ParseTokenType.COLON);
                } else if (peek() != null && peek().type == ParseTokenType.EQUALS) {
                    ttl = parseTtl(ParseTokenType.EQUALS);
                }
                try {
                    builder.addToken(tokenStr.getBytes(), ttl);
                } catch (IllegalArgumentException ex) {
                    throw error(ex.getMessage(), initialToken.offset, ex);
                }
            }
        }

        /**
         * Read a string literal.
         */
        protected String parseStrliteral() {
            ParseToken token = peek();
            String literalContent = null;

            if (token.type == ParseTokenType.STRLITERAL) {
                literalContent = expect(ParseTokenType.STRLITERAL);
                literalContent = literalContent.substring(1, literalContent.length() - 1);
            } else if (token.type == ParseTokenType.LABELED_STRLITERAL) {
                literalContent = expect(ParseTokenType.LABELED_STRLITERAL);
                String[] parts = literalContent.trim().split("\\s");
                literalContent = parts[parts.length - 1];
            }

            StringBuilder sb = new StringBuilder();
            for (int charPos = 0; charPos < literalContent.length(); charPos++) {
                char c = literalContent.charAt(charPos);
                if (c != '\\') {
                    sb.append(c);
                } else {
                    charPos++;
                    if (charPos >= literalContent.length()) {
                        throw error("Unexpected end of string literal parsing escape code", token.offset + charPos - 1);
                    }
                    c = literalContent.charAt(charPos);
                    switch (c) {
                        case '"':
                            sb.append('"');
                            break;
                        case '\\':
                            sb.append('\\');
                            break;
                        case 'b':
                            sb.append('\b');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'u': {
                            String ordTxt = literalContent.substring(charPos + 1, charPos + 5);
                            if (ordTxt.length() != 4) {
                                if (charPos >= literalContent.length()) {
                                    throw error("Unexpected end of string literal parsing escape code", token.offset + charPos - 1);
                                }
                            }
                            try {
                                sb.append((char) Integer.parseInt(ordTxt, 16));
                            } catch (Exception ex) {
                                throw error("Failed to parse string literal", token.offset + charPos - 1, ex);
                            }
                            charPos += 4;
                            break;
                        }
                        case 'x': {
                            String ordTxt = literalContent.substring(charPos + 1, charPos + 3);
                            if (ordTxt.length() != 2) {
                                if (charPos >= literalContent.length()) {
                                    throw error("Unexpected end of string literal parsing escape code", token.offset + charPos - 1);
                                }
                            }
                            try {
                                sb.append((char) Integer.parseInt(ordTxt, 16));
                            } catch (Exception ex) {
                                throw error("Failed to parse string literal", token.offset + charPos - 1, ex);
                            }
                            charPos += 2;
                            break;
                        }
                        default:
                            throw error("Unsupported escape", token.offset + charPos - 1);
                    }
                }
            }
            return sb.toString();
        }

        protected long parseTtl(ParseTokenType type) {
            ParseToken token = peek();
            expect(type);
            String ttlNum = expect(ParseTokenType.NUMBER);
            String ttlUnit = expect(ParseTokenType.UNIT);
            try {
                return Long.parseLong(ttlNum) * AgeOffPeriod.getTtlUnitsFactor(ttlUnit);
            } catch (Exception ex) {
                throw error("Failed to parse TTL", token.offset, ex);
            }
        }
    }

}
