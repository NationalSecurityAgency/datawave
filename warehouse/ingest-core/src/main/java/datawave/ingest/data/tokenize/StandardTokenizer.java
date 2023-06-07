package datawave.ingest.data.tokenize;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import datawave.util.ObjectFactory;

import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 * A grammar-based Tokenizer constructed with JFlex based on Lucene's {@link org.apache.lucene.analysis.standard.StandardTokenizer StandardTokenizer}
 * <p>
 * This Tokenizer allows a max token length to be set via {@link #setMaxTokenLength(int)}. Tokens longer than this length will be dropped. By default this limit
 * is set to 65536 characters.
 * <p>
 * This Tokenizer allows the user to specify a token truncation length. Tokens longer than this length will be truncated and have the {@code TruncatAttribute}
 * set to true. Truncation lengths can be set on a per token type basis. If truncation length isn't specified for a given type, a fallback to a default
 * truncation length will occur. By default the truncation for FILE, URL or HTTP_REQUEST tokens is 1024 characters, while the default truncation length for all
 * other types is 64 characters.
 * <p>
 * This Tokenizer can be configured to take special action when the string {@code METABREAK} is encountered as a token. If enabled via
 * {@link #setMetaBreakEnabled(boolean)}, an increment of {@link #metaBreakIncrement} will be added for the next token, and the {@code METABREAK} token will be
 * dropped. This is in place to allow KEY/VALUE pairs to be indexed as adjacent strings, but provide an effective method for separating multiple KEY/VALUE pairs
 * contained with a text stream to prevent mismatches due to overlap.
 */

public class StandardTokenizer extends Tokenizer {
    /** A private instance of the JFlex-constructed scanner */
    private Lexer scanner;
    private Class<? extends Lexer> scannerClazz = StandardLexer.class;
    
    /** Absolute maximum sized token */
    private static final int MAX_TOKEN_LENGTH_LIMIT = 8 * 1024;
    
    private int maxTokenLength = StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH;
    
    private int defaultTokenTruncateLength = StandardAnalyzer.DEFAULT_TRUNCATE_TOKEN_LENGTH;
    
    private static final String META_BREAK = "METABREAK";
    private static final char[] META_BREAK_CHARS = META_BREAK.toCharArray();
    
    private static final int DEFAULT_META_BREAK_INCREMENT = 10;
    private int metaBreakIncrement = DEFAULT_META_BREAK_INCREMENT;
    
    private static final Map<String,Integer> DEFAULT_TYPED_TOKEN_LENGTHS = new HashMap<>();
    {
        DEFAULT_TYPED_TOKEN_LENGTHS.put("<FILE>", 1024);
        DEFAULT_TYPED_TOKEN_LENGTHS.put("<URL>", 1024);
        DEFAULT_TYPED_TOKEN_LENGTHS.put("<HTTP_REQUEST>", 1024);
    }
    
    /** Token typed truncation rules. */
    private final Map<String,Integer> typedTokenTruncateLength = new HashMap<>(DEFAULT_TYPED_TOKEN_LENGTHS);
    
    // this tokenizer generates three attributes:
    // offset, positionIncrement and type
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final TruncateAttribute truncAtt = addAttribute(TruncateAttribute.class);
    
    private int skippedPositions;
    
    private boolean metaBreakEnabled = false;
    
    public StandardTokenizer() {
        init();
    }
    
    public StandardTokenizer(Class<? extends Lexer> implClass) {
        init(implClass);
    }
    
    public StandardTokenizer(AttributeFactory factory) {
        super(factory);
        init();
    }
    
    public StandardTokenizer(AttributeFactory factory, Class<? extends Lexer> implClass) {
        super(factory);
        init(implClass);
    }
    
    private void init(Class<? extends Lexer> implClass) {
        this.scannerClazz = implClass;
        init();
    }
    
    private void init() {
        final Object[] args = {input};
        this.scanner = (Lexer) ObjectFactory.create(scannerClazz.getName(), args);
        if (scanner instanceof StandardLexer) {
            StandardLexer i = (StandardLexer) scanner;
            i.setBufferSize(StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH);
        }
    }
    
    /**
     * Set the max allowed token length. Any token longer than this is skipped.
     * 
     * @param length
     *            length of token
     */
    public void setMaxTokenLength(int length) {
        if (length < 1) {
            throw new IllegalArgumentException("maxTokenLength must be greater than zero");
        }
        this.maxTokenLength = length;
    }
    
    public void setScannerBufferSize(int length) {
        
    }
    
    /**
     * @see #setMaxTokenLength
     * @return max token length
     */
    public int getMaxTokenLength() {
        return maxTokenLength;
    }
    
    /**
     * Set the token truncation length. Any token longer than this is truncated.
     * 
     * @param length
     *            length of token truncation
     */
    public void setTokenTruncateLength(int length) {
        this.defaultTokenTruncateLength = length;
    }
    
    /**
     * @see #setMaxTokenLength
     * @return token truncate length
     */
    public int getTokenTruncateLength() {
        return defaultTokenTruncateLength;
    }
    
    /** Clear the map of token type to truncationg length. */
    public void clearTokenTruncateLengths() {
        typedTokenTruncateLength.clear();
    }
    
    /**
     * Set the token truncation length for a given token type
     * 
     * @param type
     *            set the truncation length for this token type
     * @param length
     *            the truncation length for the specified type
     */
    public void setTokenTruncateLength(String type, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("Length must be zero or greater");
        }
        typedTokenTruncateLength.put(type, Integer.valueOf(length));
    }
    
    /**
     * Return the type-specifc truncation length, or -1 if no truncation length has been set for the specified type.
     * 
     * @param type
     *            get the truncation length for this token type
     * @return integer value of truncation for the specified type or -1 if there is none
     */
    public int getTokenTruncateLength(String type) {
        Integer val = typedTokenTruncateLength.get(type);
        if (val == null) {
            return -1;
        } else {
            return val.intValue();
        }
    }
    
    /**
     * @return true if metatata breaking is enabled
     */
    public boolean isMetaBreakEnabled() {
        return metaBreakEnabled;
    }
    
    /**
     * @param metaBreakEnabled
     *            enable / disable metadata breaking
     */
    public void setMetaBreakEnabled(boolean metaBreakEnabled) {
        this.metaBreakEnabled = metaBreakEnabled;
    }
    
    /**
     * @return the position increment to use when inserting a metadata break
     */
    public int getMetaBreakIncrement() {
        return this.metaBreakIncrement;
    }
    
    /**
     * @param increment
     *            the position increment to use when inserting a metadata break
     */
    public void setMetaBreakIncrement(int increment) {
        this.metaBreakIncrement = increment;
    }
    
    @Override
    public final boolean incrementToken() throws IOException {
        clearAttributes();
        skippedPositions = 0;
        
        while (true) {
            int tokenType = scanner.getNextToken();
            
            if (tokenType == StandardLexer.YYEOF) {
                return false;
            } else if (metaBreakEnabled && isMetaBreak(scanner)) {
                skippedPositions += metaBreakIncrement;
            } else if (scanner.yylength() <= maxTokenLength) {
                posIncrAtt.setPositionIncrement(skippedPositions + 1);
                typeAtt.setType(Lexer.TOKEN_TYPES[tokenType]);
                int truncateLength = getTokenTruncateLength(typeAtt.type());
                if (truncateLength < 0) {
                    truncateLength = defaultTokenTruncateLength;
                }
                scanner.getText(termAtt, truncateLength);
                final int start = scanner.yychar();
                offsetAtt.setOffset(correctOffset(start), correctOffset(start + termAtt.length()));
                truncAtt.setTruncated(scanner.yylength() > truncateLength);
                truncAtt.setOriginalLength(scanner.yylength());
                return true;
            } else {
                // When we skip term that's too long, we still increment the position
                skippedPositions++;
            }
        }
    }
    
    /**
     * @param scanner
     *            The scanner to check
     * @return true if the scanner is positioned at a meta break character.
     */
    private static final boolean isMetaBreak(Lexer scanner) {
        if (scanner.yylength() != META_BREAK_CHARS.length) {
            return false;
        }
        
        for (int i = 0; i < META_BREAK_CHARS.length; i++) {
            if (META_BREAK_CHARS[i] != scanner.yycharat(i)) {
                return false;
            }
        }
        
        return true;
    }
    
    @Override
    public final void end() throws IOException {
        super.end();
        // set final offset
        int finalOffset = correctOffset(scanner.yychar() + scanner.yylength());
        offsetAtt.setOffset(finalOffset, finalOffset);
        // adjust any skipped tokens
        posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        scanner.yyreset(input);
    }
    
    @Override
    public void reset() throws IOException {
        super.reset();
        scanner.yyreset(input);
        skippedPositions = 0;
    }
}
