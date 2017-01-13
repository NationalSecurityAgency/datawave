package nsa.datawave.ingest.data.tokenize;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public abstract class Lexer {
    public static final int ALPHANUM = 0;
    public static final int APOSTROPHE = 1;
    public static final int ACRONYM = 2;
    public static final int EMAIL = 3;
    public static final int HOST = 4;
    public static final int IP_ADDR = 5;
    public static final int NUM = 6;
    public static final int FILE = 7;
    public static final int URL = 8;
    public static final int TIMESTAMP = 9;
    public static final int UNDERSCORE = 10;
    public static final int HTTP_REQUEST = 11;
    public static final int SYMBOL = 12;
    
    public static final String[] TOKEN_TYPES = new String[] {"<ALPHANUM>", "<APOSTROPHE>", "<ACRONYM>", "<EMAIL>", "<HOST>", "<IP_ADDR>", "<NUM>", "<FILE>",
            "<URL>", "<TIMESTAMP>", "<UNDERSCORE>", "<HTTP_REQUEST>", "<SYMBOL>"};
    
    abstract public int getNextToken() throws java.io.IOException;
    
    abstract public int yychar();
    
    abstract void getText(CharTermAttribute t, int maxLength);
    
    abstract public void yyreset(java.io.Reader reader);
    
    abstract int yylength();
    
    abstract public char yycharat(int i);
}
