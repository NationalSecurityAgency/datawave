package datawave.ingest.data.tokenize;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.CharArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTokenSearch implements TokenSearch {
    private static Logger logger = LoggerFactory.getLogger(DefaultTokenSearch.class);

    private static final HashMap<String,CharArraySet> stopwordCache = new HashMap<>();

    protected final CharArraySet stopwords;

    protected boolean reverse = false;

    protected boolean emailDomainTokensEnabled = true;

    protected boolean dirtyWordTokensEnabled = true;
    protected boolean fileWordTokensEnabled = true;
    protected boolean emailWordTokensEnabled = true;
    protected boolean urlWordTokensEnabled = true;
    protected boolean termWordTokensEnabled = true;

    protected int maxUrlDecodes = 1;

    private Pattern tokenWordPtrn = Pattern.compile("[\\p{Punct}\\p{Space}\\p{Cntrl}]+");
    private Pattern dirtyTokensPtrn = Pattern.compile("[ &'\"@\\.]");

    public DefaultTokenSearch() throws IOException {
        this(getStopWords());
    }

    public DefaultTokenSearch(CharArraySet stopwords) {
        this(stopwords, false);
    }

    public DefaultTokenSearch(boolean reverse) throws IOException {
        this(getStopWords(), reverse);
    }

    public DefaultTokenSearch(CharArraySet stopwords, boolean reverse) {
        this.stopwords = stopwords;
        this.reverse = reverse;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#getInstanceStopwords()
     */
    @Override
    public CharArraySet getInstanceStopwords() {
        return stopwords;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#setReverse(boolean)
     */
    @Override
    public void setReverse(boolean reverse) {
        this.reverse = reverse;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#isReverse()
     */
    @Override
    public boolean isReverse() {
        return reverse;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#isEmailDomainTokensEnabled()
     */
    @Override
    public boolean isEmailDomainTokensEnabled() {
        return emailDomainTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#setEmailDomainTokensEnabled(boolean)
     */
    @Override
    public void setEmailDomainTokensEnabled(boolean emailDomainTokensEnabled) {
        this.emailDomainTokensEnabled = emailDomainTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#isDirtyWordTokensEnabled()
     */
    @Override
    public boolean isDirtyWordTokensEnabled() {
        return dirtyWordTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#setDirtyWordTokensEnabled(boolean)
     */
    @Override
    public void setDirtyWordTokensEnabled(boolean dirtyWordsTokensEnabled) {
        this.dirtyWordTokensEnabled = dirtyWordsTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#isFileWordTokensEnabled()
     */
    @Override
    public boolean isFileWordTokensEnabled() {
        return fileWordTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#setFileWordTokensEnabled(boolean)
     */
    @Override
    public void setFileWordTokensEnabled(boolean fileWordTokensEnabled) {
        this.fileWordTokensEnabled = fileWordTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#isEmailWordTokensEnabled()
     */
    @Override
    public boolean isEmailWordTokensEnabled() {
        return emailWordTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#setEmailWordTokensEnabled(boolean)
     */
    @Override
    public void setEmailWordTokensEnabled(boolean emailWordTokensEnabled) {
        this.emailWordTokensEnabled = emailWordTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#isUrlWordTokensEnabled()
     */
    @Override
    public boolean isUrlWordTokensEnabled() {
        return urlWordTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#setUrlWordTokensEnabled(boolean)
     */
    @Override
    public void setUrlWordTokensEnabled(boolean urlWordTokensEnabled) {
        this.urlWordTokensEnabled = urlWordTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#isTermWordTokensEnabled()
     */
    @Override
    public boolean isTermWordTokensEnabled() {
        return termWordTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#setTermWordTokensEnabled(boolean)
     */
    @Override
    public void setTermWordTokensEnabled(boolean termWordTokensEnabled) {
        this.termWordTokensEnabled = termWordTokensEnabled;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#setMaxURLDecodes(int)
     */
    @Override
    public void setMaxURLDecodes(int maxUrlDecodes) {
        this.maxUrlDecodes = maxUrlDecodes;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#getMaxURLDecodes()
     */
    @Override
    public int getMaxURLDecodes() {
        return this.maxUrlDecodes;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#getSynonyms(java.lang.String[], java.lang.String, boolean)
     */
    @Override
    public Collection<String> getSynonyms(String[] zw, String termType, boolean includeTerm) {
        if (termType.equals("<EMAIL>") || termType.equals("<HOST>")) {
            return emailAddressTokens(zw, reverse, includeTerm);
        } else if (termType.equals("<IP_ADDR>")) {
            return ipAddressTokens(zw, reverse, includeTerm);
        } else if (termType.equals("<URL>")) {
            return urlTokens(zw, reverse, includeTerm);
        } else if (termType.equals("<FILE>")) {
            return filePathTokens(zw, reverse, includeTerm);
        } else if (termType.equals("<HTTP_REQUEST>")) {
            return httpRequestTokens(zw, reverse, includeTerm);
        } else if (termType.equals("<APOSTROPHE>") || termType.equals("<ACRONYM>") || termType.equals("<COMPANY>")) {
            return dirtyTokens(zw, includeTerm);
        } else if (termType.equals("<TIMESTAMP>")) {
            return timestampTokens(zw, reverse, includeTerm);
        } else // ALPHANUM, NUM, TIMESTAMP, UNDERSCORE handled here.
        {
            return getTermSynonyms(zw, includeTerm);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#getSynonyms(java.lang.String, java.lang.String, boolean)
     */
    @Override
    public List<String> getSynonyms(String term, String termType, boolean includeTerm) {
        return new ArrayList<>(getSynonyms(dezone(term), termType, includeTerm));
    }

    protected static String[] dezone(String term) {
        int pos = term.lastIndexOf(":");
        String word = "";
        String zone = "";
        if (pos > -1) {
            zone = term.substring(pos);
            word = term.substring(0, pos);
        } else {
            word = term;
        }

        return new String[] {word, zone};
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#getTokenWords(java.lang.String, java.lang.String, java.util.Collection)
     */
    @Override
    public void getTokenWords(String input, String zone, Collection<String> synonyms) {
        // Now treat it as a bucket of words so that double quotes phrases can work too
        for (String word : getTokenWords(input)) {
            if (!word.isEmpty() && !isStop(word) && !word.equals(input)) {
                synonyms.add(word.toLowerCase() + zone);
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#getTokenWords(java.lang.String)
     */
    @Override
    public String[] getTokenWords(String input) {
        return tokenWordPtrn.split(input, 0);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#emailAddressTokens(java.lang.String[], boolean, boolean)
     */
    @Override
    public Collection<String> emailAddressTokens(String[] zw, boolean reverse_indexing, boolean includeTerm) {
        Set<String> synonyms = new LinkedHashSet<>();

        String addr = zw[0];
        String zone = zw[1];

        String lcaddr = addr.toLowerCase();

        // include downcased version if it is different or if includeTerm is
        // flagged
        if ((!addr.equals(lcaddr)) || includeTerm) {
            synonyms.add(lcaddr + zone);
        }

        int atpos = addr.indexOf("@");
        if (atpos > 0) {
            String name = lcaddr.substring(0, atpos);
            String domain = lcaddr.substring(atpos);
            synonyms.add(name + zone);

            if (emailDomainTokensEnabled) {
                synonyms.add(domain + zone);
            }

            if (emailWordTokensEnabled) {
                getTokenWords(name, zone, synonyms);
            }

            if (domain.equals("@gmail.com")) {
                String me_name = name.replaceAll("[\\.]", "").replaceAll("\\+.*", "");
                if (!me_name.equals(name)) {
                    synonyms.add(me_name + domain + zone);
                }
            }
        } else if (emailWordTokensEnabled) {
            getTokenWords(lcaddr, zone, synonyms);
        }

        if (lcaddr.lastIndexOf(".") <= lcaddr.length() - 7) {
            // hedge against things improperly identified as e-mail addresses
            // by always tokenizing e-mail addresses with long ending domains.
            getTokenWords(lcaddr, zone, synonyms);
        }

        return synonyms;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#emailAddressTokens(java.lang.String, boolean, boolean)
     */
    @Override
    public List<String> emailAddressTokens(String term, boolean reverse_indexing, boolean includeTerm) {
        return new ArrayList<>(emailAddressTokens(dezone(term), reverse_indexing, includeTerm));
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#ipAddressTokens(java.lang.String[], boolean, boolean)
     */
    @Override
    public Collection<String> ipAddressTokens(String[] zw, boolean reverse_indexing, boolean includeTerm) {
        Set<String> synonyms = new LinkedHashSet<>();

        String addr = zw[0];
        String zone = zw[1];

        String lcaddr = addr.toLowerCase();

        // include the downcased term if it is different from the
        // original form or the include term flag is raised.
        if ((!addr.equals(lcaddr)) || includeTerm) {
            synonyms.add(lcaddr + zone);
        }

        return synonyms;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#ipAddressTokens(java.lang.String, boolean, boolean)
     */
    @Override
    public List<String> ipAddressTokens(String term, boolean reverse_indexing, boolean includeTerm) {
        return new ArrayList<>(ipAddressTokens(dezone(term), reverse_indexing, includeTerm));
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#timestampTokens(java.lang.String, boolean, boolean)
     */
    @Override
    public List<String> timestampTokens(String term, boolean reverse_indexing, boolean includeTerm) {
        return new ArrayList<>(timestampTokens(dezone(term), reverse_indexing, includeTerm));
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#timestampTokens(java.lang.String[], boolean, boolean)
     */
    @Override
    public Collection<String> timestampTokens(String[] zw, boolean reverse_indexing, boolean includeTerm) {
        if (reverse_indexing)
            return Collections.emptyList();

        Set<String> synonyms = new LinkedHashSet<>();

        String tstamp = zw[0].toLowerCase();
        String zone = zw[1];

        String lctstamp = tstamp.toLowerCase();

        if ((!tstamp.equals(lctstamp)) || includeTerm) {
            synonyms.add(lctstamp + zone);
        }

        tstamp = lctstamp; // reuse tstamp for edits

        // extract the y,m,d portion prior to t.
        int tPos = tstamp.indexOf("t");
        if (tPos > 0) {
            String daystamp = tstamp.substring(0, tPos);
            synonyms.add(daystamp + zone);
            tstamp = tstamp.substring(0, tPos);
        }

        // todo: something with the time and zone here.

        // add what remains as well.
        if (!tstamp.equals(lctstamp)) {
            synonyms.add(tstamp + zone);
        }

        return synonyms;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#filePathTokens(java.lang.String[], boolean, boolean)
     */
    @Override
    public Collection<String> filePathTokens(String[] zw, boolean reverse_indexing, boolean includeTerm) {
        // Nothing to do if reverse indexing
        if (reverse_indexing)
            return Collections.emptyList();

        Set<String> synonyms = new LinkedHashSet<>();

        String zlc = zw[0].toLowerCase();
        String zone = zw[1];

        // emit non-path normalized, lc version first if it is
        // different from the base form or include term is flagged.
        if ((!zlc.equals(zw[0])) || includeTerm) {
            synonyms.add(zlc + zone);
        }

        // Path normalization
        if (zlc.indexOf("\\") > -1) {
            // emit path normalized, lc version.
            // yes, this regex really matches one or more '\' characters.
            zlc = zlc.replaceAll("\\\\+", "/");
            synonyms.add(zlc + zone);
        }

        // Replace runs of consecutive slashes with a single slash.
        if (zlc.indexOf("//") > -1) {
            zlc = zlc.replaceAll("/+", "/");
        }

        // Remove trailing slashes.
        if (zlc.endsWith("/")) {
            zlc = zlc.substring(0, zlc.length() - 1);
        }

        // space separated directory walk fix
        // e.g: c:/program files/foo/bar.exe bar~2.exe
        // not: c:/program files/temp .net files/foobar
        // easier to fix this here than re-process the buds.
        String extra = null;
        int startPos = zlc.length();
        int spacePos = zlc.lastIndexOf(" ", startPos);
        int slashPos = zlc.lastIndexOf("/", startPos);

        // working from the end to the beginning, look at spaces
        // prior to the final slash. Check the tokens on either side
        // of the space to see if the left side has something that looks
        // like a file extension >or< the right side has something that
        // looks like an 8.3 filename. If any of these are true, we've found
        // our delimiter.
        while (spacePos > slashPos + 1) {
            if ((spacePos >= 4 && '.' == zlc.charAt(spacePos - 4)) || (startPos >= 2 && '~' == zlc.charAt(startPos - 2))
                            || (startPos >= 6 && '~' == zlc.charAt(startPos - 6))) {
                extra = zlc.substring(spacePos + 1);
                synonyms.add(extra + zone); // e.g barca~2.exe
                zlc = zlc.substring(0, spacePos); // everything else
                synonyms.add(zlc + zone);
                break;
            }
            // next space.
            startPos = spacePos - 1;
            spacePos = zlc.lastIndexOf(" ", startPos);
        }

        // File path without drive letter
        if (zlc.indexOf(":/") == 1 && zlc.length() > 4) {
            synonyms.add(zlc.substring(3) + zone);
        }

        // lowercased portions of trailing segments
        // (to avoid leading wildcard search)
        slashPos = zlc.indexOf("/");
        int segstart = 0;
        String zseg;

        // extract path segments, and trailing path segments.
        // trailing path segments eliminate the need for leading wildcard search,
        // while path segments allow more natural search upon filenames with spaces.
        while (segstart <= slashPos || segstart < zlc.length()) {
            if (segstart <= slashPos) {
                // slash delimited components of zlc
                zseg = zlc.substring(segstart, slashPos);
            } else // assume (segstart < zlc.length())
            {
                zseg = zlc.substring(segstart);
            }

            if (!zseg.isEmpty()) {
                synonyms.add(zseg + zone);

                spacePos = zseg.indexOf(" ");
                if (spacePos > -1) {
                    // extract space delimited components of zseg.
                    int zsegstart = 0;
                    while (zsegstart <= spacePos && spacePos < zseg.length()) {
                        synonyms.add(zseg.substring(zsegstart, spacePos) + zone);
                        zsegstart = spacePos + 1;
                        spacePos = zseg.indexOf(" ", zsegstart);
                    }

                    if (zsegstart < zseg.length()) {
                        synonyms.add(zseg.substring(zsegstart) + zone);
                    }
                }
            }

            // if there are more path components to process.
            if (segstart <= slashPos) {
                segstart = slashPos + 1;
                synonyms.add(zlc.substring(segstart) + zone); // trailing bits of the path.
                slashPos = zlc.indexOf("/", segstart);
            } else {
                break;
            }
        }

        if (fileWordTokensEnabled) {
            getTokenWords(zlc, zone, synonyms);
            if (extra != null) {
                getTokenWords(extra, zone, synonyms);
            }
        }

        return synonyms;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#filePathTokens(java.lang.String, boolean, boolean)
     */
    @Override
    public List<String> filePathTokens(String term, boolean reverse_indexing, boolean includeTerm) {
        return new ArrayList<>(filePathTokens(dezone(term), reverse_indexing, includeTerm));
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#httpRequestTokens(java.lang.String[], boolean, boolean)
     */
    @Override
    public Collection<String> httpRequestTokens(String[] zw, boolean reverse_indexing, boolean includeTerm) {
        // Nothing to do if reverse indexing
        if (reverse_indexing)
            return Collections.emptyList();

        Set<String> synonyms = new LinkedHashSet<>();

        String lc = zw[0].toLowerCase();
        String zone = zw[1];

        // Include the downcased request if different from the original term
        // or includeTerm is raised.
        if ((!lc.equals(zw[0])) || includeTerm) {
            synonyms.add(lc + zone);
        }

        int methodPos = lc.indexOf(" ");
        int versionPos = lc.indexOf(" http", methodPos + 1);

        if (methodPos > 0 && versionPos > methodPos) {
            String method = lc.substring(0, methodPos);
            String requestUri = lc.substring(methodPos + 1, versionPos);
            String version = lc.substring(versionPos + 1);

            synonyms.add(method + zone);
            synonyms.addAll(urlTokens(new String[] {requestUri, zone}, false, includeTerm));
            synonyms.add(version + zone);

            versionPos = version.indexOf("/");
            if (versionPos > 0) {
                synonyms.add(version.substring(0, versionPos) + zone);
            }
        } else { // can't parse, treat as a URL
            synonyms.addAll(urlTokens(zw, false, includeTerm));
        }

        return synonyms;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#httpRequestTokens(java.lang.String, boolean, boolean)
     */
    @Override
    public List<String> httpRequestTokens(String term, boolean reverse_indexing, boolean includeTerm) {
        return new ArrayList<>(httpRequestTokens(dezone(term), reverse_indexing, includeTerm));
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#dirtyTokens(java.lang.String[], boolean)
     */
    @Override
    public Collection<String> dirtyTokens(String[] zw, boolean includeTerm) {
        Set<String> synonyms = new LinkedHashSet<>();

        String lc = zw[0].toLowerCase();
        String zone = zw[1];

        // Include the downcased original if different from the original term
        // or includeTerm is raised.
        if ((!lc.equals(zw[0])) || includeTerm) {
            synonyms.add(lc + zone);
        }

        // Include the synonyms of the 'original with dirtyTokens removed'.
        synonyms.addAll(getTermSynonyms(dirtyTokensPtrn.matcher(zw[0]).replaceAll("") + zone, includeTerm));
        if (dirtyWordTokensEnabled) {
            getTokenWords(zw[0], zw[1], synonyms);
        }

        return synonyms;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#dirtyTokens(java.lang.String, boolean)
     */
    @Override
    public List<String> dirtyTokens(String term, boolean includeTerm) {
        return new ArrayList<>(dirtyTokens(dezone(term), includeTerm));
    }

    private final String urlDecode(String input) throws UnsupportedEncodingException {
        // the url we receive may be truncated, so look back 2 characters for a
        // '%' and trim it if it exists.
        for (int i = input.length() - 1; i > input.length() - 3 && i >= 0; i--) {
            if (input.charAt(i) == '%') {
                input = input.substring(0, i);
                break;
            }
        }
        return java.net.URLDecoder.decode(input, "UTF-8");
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#urlTokens(java.lang.String[], boolean, boolean)
     */
    @Override
    public Collection<String> urlTokens(String[] zw, boolean reverse_indexing, boolean includeTerm) {
        // Nothing to do if reverse indexing
        if (reverse_indexing)
            return Collections.emptyList();

        Set<String> synonyms = new LinkedHashSet<>();

        String lc_url = zw[0].toLowerCase();
        String zone = zw[1];
        String prevUrl;
        int loopCount = 0;

        do {
            prevUrl = lc_url.toString();

            int querypos = lc_url.indexOf("?");
            int schemepos = lc_url.indexOf("://");
            int fragpos = lc_url.indexOf("#");

            // Include the downcased url if different from the original term
            // or includeTerm is raised.
            if ((!lc_url.equals(zw[0])) || includeTerm) {
                synonyms.add(lc_url + zone);
            }

            // if there is a scheme://host:port/
            if (schemepos > -1 && lc_url.length() > schemepos + 4) {
                // Add url without scheme
                synonyms.add(lc_url.substring(schemepos + 3) + zone);

                // Just the URL path, excluding host, including query and fragment strings.
                int pathstart = lc_url.indexOf("/", schemepos + 4);
                if (pathstart > -1) {
                    synonyms.add(lc_url.substring(pathstart) + zone);

                    // without fragment
                    if (fragpos > pathstart) {
                        synonyms.add(lc_url.substring(pathstart, fragpos) + zone);
                    }

                    // without query or fragment
                    if (querypos > pathstart) {
                        synonyms.add(lc_url.substring(pathstart, querypos) + zone);
                    }

                    // Just the host or ip
                    int portpos = lc_url.indexOf(":", schemepos + 4);
                    if (portpos > schemepos && portpos < pathstart) {
                        synonyms.add(lc_url.substring(schemepos + 3, portpos) + zone);
                    } else {
                        synonyms.add(lc_url.substring(schemepos + 3, pathstart) + zone);
                    }
                }
            }

            // Index the the fragment separately if there is one
            if (fragpos > -1) {
                synonyms.add(lc_url.substring(0, fragpos) + zone);
                if (fragpos + 1 < lc_url.length()) {
                    String frag = lc_url.substring(fragpos + 1);

                    synonyms.add(frag + zone);
                }
            }

            // Index the query string separately if there is one.
            if (querypos > -1 && querypos < lc_url.length() - 1) {
                int endq = fragpos > querypos ? fragpos : lc_url.length();
                synonyms.add(lc_url.substring(0, querypos) + zone);
                String params[] = lc_url.substring(querypos + 1, endq).split("&");
                for (String param : params) {
                    if (!param.isEmpty()) {
                        try {
                            String decoded = urlDecode(param);
                            synonyms.add(decoded + zone);

                            // See if we have an embedded url or email address as a parameter
                            if (param.indexOf("=") > 0) {
                                String kv[] = decoded.split("=", 2);
                                if (kv != null && kv.length == 2 && kv[1].length() > 4) {
                                    if (kv[1].startsWith("http://") || kv[1].startsWith("https://")) {
                                        synonyms.addAll(urlTokens(new String[] {kv[1], zone}, false, includeTerm));
                                    } else if (kv[1].indexOf("@") > 0) {
                                        int atPos = kv[1].indexOf("@");
                                        synonyms.add(kv[1] + zone);
                                        if (atPos + 1 < kv[1].length()) {
                                            synonyms.add(kv[1].substring(atPos) + zone);
                                        }
                                    }
                                }
                            }
                        } catch (Throwable t) {
                            logger.warn("Exception caught when URLDecoding parameter, using encoded version: " + param, t);
                            synonyms.add(param + zone);
                        }
                    }

                }
            }

            // Decompose the url to find the path and then chop off the leading
            // bits to produce a series of tokens include sub-paths. Eventually
            // this should produce the filename referenced by the url.
            String url_path = lc_url;

            // trim fragpos, this should appear after the querypos, so we do
            // it first.
            if (fragpos > -1) {
                url_path = url_path.substring(0, fragpos);
            }

            // trim querypos that appears in the string prior to the fragpos
            if (querypos > -1 && querypos < url_path.length() && ((querypos <= fragpos) || fragpos < 0)) {
                url_path = url_path.substring(0, querypos);
            }

            // trim leading scheme
            if (schemepos > -1 && schemepos + 3 < url_path.length()) {
                url_path = url_path.substring(schemepos + 3);
            }

            // collapse multiple slashes.
            if (url_path.indexOf("//") > -1) {
                url_path = url_path.replaceAll("/+", "/");
            }

            // trim trailing slash.
            if (url_path.endsWith("/")) {
                url_path = url_path.substring(0, url_path.length() - 1);
            }

            // trim leading path segments, adding each as a synonym.
            int slashpos = url_path.indexOf("/");
            while (slashpos > -1 && (slashpos < (url_path.length() - 1))) {
                synonyms.add(url_path.substring(slashpos + 1) + zone);
                slashpos = url_path.indexOf("/", slashpos + 1);
            }

            // Index words from this url
            if (urlWordTokensEnabled) {
                getTokenWords(lc_url, zone, synonyms);
            }

            try {
                lc_url = urlDecode(lc_url);
            } catch (Throwable t) {
                logger.warn("Exception caught when URLDecoding url, using encoded version: " + lc_url, t);
            }

        } while (!lc_url.toString().equals(prevUrl.toString()) && ++loopCount <= maxUrlDecodes);

        return synonyms;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#urlTokens(java.lang.String, boolean, boolean)
     */
    @Override
    public List<String> urlTokens(String term, boolean reverse_indexing, boolean includeTerm) {
        return new ArrayList<>(urlTokens(dezone(term), reverse_indexing, includeTerm));
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#getTermSynonyms(java.lang.String)
     */
    @Override
    public List<String> getTermSynonyms(String term) {
        return getTermSynonyms(term, true);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#isStop(java.lang.String)
     */
    @Override
    public boolean isStop(String term) {
        int pos = term.lastIndexOf(":");
        if (pos > -1) {
            term = term.substring(0, pos);
        }
        return stopwords.contains(term);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#getTermSynonyms(java.lang.String[], boolean)
     */
    @Override
    public Collection<String> getTermSynonyms(String[] term, boolean includeTerm) {
        Set<String> synonyms = new LinkedHashSet<>();

        if (term[1] == null)
            term[1] = "";

        // Include the downcased term if it is different from the original
        // token or if the includeTerm flag is raised.
        String lctok = term[0].toLowerCase();
        if (!isStop(lctok) && (!term[0].equals(lctok) || includeTerm)) {
            synonyms.add(lctok + term[1]);
        }

        // Check term with diacritics stripped
        String stok = AccentFilter.strip(term[0]);
        if (!term[0].equals(stok) && !isStop(stok)) {
            // Check lowercase stripped term
            String lcstok = stok.toLowerCase();
            if (!isStop(lcstok)) {
                synonyms.add(lcstok + term[1]);
            }
        }

        // UNDERSCORE, ALPHANUM, NUM and other token types that arrive here
        // may have internal non-alpha characters.
        if (termWordTokensEnabled) {
            getTokenWords(lctok, term[1], synonyms);
        } else if (term[0].indexOf('_') > -1) {
            // UNDERSCORE is always word tokenized to preserve backwards compatibility
            getTokenWords(lctok, term[1], synonyms);
        }

        return synonyms;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.data.tokenize.TokenSearch#getTermSynonyms(java.lang.String, boolean)
     */
    @Override
    public List<String> getTermSynonyms(String term, boolean includeTerm) {
        return new ArrayList<>(getTermSynonyms(dezone(term), includeTerm));
    }

    /**
     * Obtain a reference to a stopword set, referenced by the system property <code>STOP_WORD_LIST</code> or <code>stopwords.txt</code> if the property is not
     * set.
     * <p>
     * If no stopwords are found in the cache for the specified filename, this method will invoke {@link #getStopWords(String)} to load the list.
     *
     * @return a reference to a stopword set
     * @throws IOException
     *             if there is an issue getting the stop words file
     */
    public static synchronized CharArraySet getStopWords() throws IOException {
        String stopResource = System.getProperty("STOP_WORD_LIST", "stopwords.txt");
        return getStopWords(stopResource);
    }

    /**
     * Obtain a reference to a stopword set, possibly cached.
     * <p>
     * If no stopwords are found in the cache for the specified filename, this method will invoke {@link Factory#loadStopWords(String)} to load the list.
     *
     * @param filename
     *            name of the file
     * @throws IOException
     *             if there is an issue getting the stop words file
     * @return reference to stopword set
     */
    public static synchronized CharArraySet getStopWords(String filename) throws IOException {
        CharArraySet stopwords = stopwordCache.get(filename);
        if (stopwords == null) {
            stopwords = TokenSearch.Factory.loadStopWords(filename);
            stopwordCache.put(filename, stopwords);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Using " + stopwords.size() + " stopwords from " + filename);
        }
        return stopwords;
    }
}
