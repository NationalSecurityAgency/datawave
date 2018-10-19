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
import java.io.Reader;
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
 * other types is 50.
 * <p>
 * This Tokenizer will can be condfigured to take special action when the string {@code METABREAK} is encoutnered as a token. If enabled via
 * {@link #setMetaBreakEnabled(boolean)}, an increment of {@link #metaBreakIncrement} will be added for the next token, and the {@code METABREAK} token will be
 * dropped. This is in place to allow KEY/VALUE pairs to be indexed as adjacent strings, but provide an effective method for separating multiple KEY/VALUE pairs
 * contained with a text stream to prevent mismatches due to overlap.
 */

public class StandardTokenizer extends Tokenizer {
    /** A private instance of the JFlex-constructed scanner */
    private final Lexer scanner;
    
    private static final int DEFAULT_MAX_TOKEN_LENGTH = 8 * 1024;
    private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;
    
    private static final int DEFAULT_TRUNCATE_TOKEN_LENGTH = 50;
    private int defaultTokenTruncateLength = DEFAULT_TRUNCATE_TOKEN_LENGTH;
    
    public static final String META_BREAK = "METABREAK";
    private static final char[] META_BREAK_CHARS = META_BREAK.toCharArray();
    
    private static final int DEFAULT_META_BREAK_INCREMENT = 10;
    private int metaBreakIncrement = DEFAULT_META_BREAK_INCREMENT;
    
    private static final Map<String,Integer> DEFAULT_TYPED_TOKEN_LENGTHS = new HashMap<>();
    {
        DEFAULT_TYPED_TOKEN_LENGTHS.put("<FILE>", Integer.valueOf(1024));
        DEFAULT_TYPED_TOKEN_LENGTHS.put("<URL>", Integer.valueOf(1024));
        DEFAULT_TYPED_TOKEN_LENGTHS.put("<HTTP_REQUEST>", Integer.valueOf(1024));
    }
    
    /** Token typed truncation rules. */
    private final Map<String,Integer> typedTokenTruncateLength = new HashMap<>(DEFAULT_TYPED_TOKEN_LENGTHS);
    
    // this tokenizer generates three attributes:
    // offset, positionIncrement and type
    private CharTermAttribute termAtt;
    private OffsetAttribute offsetAtt;
    private PositionIncrementAttribute posIncrAtt;
    private TypeAttribute typeAtt;
    private TruncateAttribute truncAtt;
    
    private boolean metaBreakEnabled = false;
    
    /**
     * Creates a new instance of the {@link org.apache.lucene.analysis.standard.StandardTokenizer}. Attaches the {@code input} to the newly created JFlex
     * scanner.
     *
     * @param input
     *            The input reader
     *
     *            See http://issues.apache.org/jira/browse/LUCENE-1068
     */
    public StandardTokenizer(Reader input) {
        super();
        setReader(input);
        this.scanner = new StandardLexer(input);
        init();
    }
    
    public StandardTokenizer(Reader input, Class<? extends Lexer> implClass) {
        super();
        setReader(input);
        Object[] args = {input};
        this.scanner = (Lexer) ObjectFactory.create(implClass.getName(), args);
        init();
    }
    
    public StandardTokenizer(AttributeFactory factory, Reader input, Class<? extends Lexer> implClass) {
        super(factory);
        setReader(input);
        Object[] args = {input};
        this.scanner = (Lexer) ObjectFactory.create(implClass.getName(), args);
        init();
    }
    
    /**
     * Creates a new StandardTokenizer with a given {@link org.apache.lucene.util.AttributeSource.AttributeFactory}
     */
    public StandardTokenizer(AttributeFactory factory, Reader input) {
        super(factory);
        setReader(input);
        this.scanner = new StandardLexer(input);
        init();
    }
    
    private void init() {
        termAtt = addAttribute(CharTermAttribute.class);
        offsetAtt = addAttribute(OffsetAttribute.class);
        posIncrAtt = addAttribute(PositionIncrementAttribute.class);
        typeAtt = addAttribute(TypeAttribute.class);
        truncAtt = addAttribute(TruncateAttribute.class);
        if (scanner instanceof StandardLexer) {
            StandardLexer i = (StandardLexer) scanner;
            i.setBufferSize(DEFAULT_MAX_TOKEN_LENGTH);
        }
    }
    
    /**
     * Set the max allowed token length. Any token longer than this is skipped.
     */
    public void setMaxTokenLength(int length) {
        if (length < 1) {
            throw new IllegalArgumentException("maxTokenLength must be greater than zero");
        }
        this.maxTokenLength = length;
    }
    
    public void setScannerBufferSize(int length) {
        
    }
    
    /** @see #setMaxTokenLength */
    public int getMaxTokenLength() {
        return maxTokenLength;
    }
    
    /**
     * Set the token truncation length. Any token longer than this is truncated.
     */
    public void setTokenTruncateLength(int length) {
        this.defaultTokenTruncateLength = length;
    }
    
    /** @see #setMaxTokenLength */
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
     * @param length
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
    
    public boolean isMetaBreakEnabled() {
        return metaBreakEnabled;
    }
    
    public void setMetaBreakEnabled(boolean metaBreakEnabled) {
        this.metaBreakEnabled = metaBreakEnabled;
    }
    
    public int getMetaBreakIncrement() {
        return this.metaBreakIncrement;
    }
    
    public void setMetaBreakIncrement(int increment) {
        this.metaBreakIncrement = increment;
    }
    
    @Override
    public final boolean incrementToken() throws IOException {
        clearAttributes();
        int posIncr = 1;
        
        while (true) {
            int tokenType = scanner.getNextToken();
            
            if (tokenType == StandardLexer.YYEOF) {
                return false;
            } else if (metaBreakEnabled && isMetaBreak(scanner)) {
                posIncr += metaBreakIncrement;
            } else if (scanner.yylength() <= maxTokenLength) {
                posIncrAtt.setPositionIncrement(posIncr);
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
                posIncr++;
            }
        }
    }
    
    /**
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
    }
}
