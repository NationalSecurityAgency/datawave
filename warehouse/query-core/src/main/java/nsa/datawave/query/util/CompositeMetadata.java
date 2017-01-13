package nsa.datawave.query.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

public class CompositeMetadata {
    
    private static final Logger log = Logger.getLogger(CompositeMetadata.class);
    
    private Set<String> ingestTypes = Sets.newHashSet();
    
    protected LoadingCache<String,Multimap<String,String>> compositeMetadata = CacheBuilder.newBuilder().build(
                    new CacheLoader<String,Multimap<String,String>>() {
                        @Override
                        public Multimap<String,String> load(String key) throws Exception {
                            return HashMultimap.<String,String> create();
                        }
                    });
    
    protected LoadingCache<String,Multimap<String,String>> compositeToFieldMap = CacheBuilder.newBuilder().build(
                    new CacheLoader<String,Multimap<String,String>>() {
                        @Override
                        public Multimap<String,String> load(String key) throws Exception {
                            return ArrayListMultimap.<String,String> create();
                        }
                    });
    
    public CompositeMetadata() {}
    
    public CompositeMetadata(String in) {
        this.fromString(in);
    }
    
    public CompositeMetadata(CompositeMetadata in) {
        this.compositeMetadata.putAll(in.compositeMetadata.asMap());
        this.ingestTypes.addAll(in.ingestTypes);
    }
    
    public void addForAllIngestTypes(Map<String,Set<String>> map) {
        for (String fieldName : map.keySet()) {
            for (String ingestType : ingestTypes) {
                this.put(fieldName, ingestType, map.get(fieldName));
            }
        }
    }
    
    public CompositeMetadata put(String compositeName, String ingestType, Collection<String> fields) {
        addCompositeMetadata(compositeName, ingestType, fields);
        return this;
    }
    
    public CompositeMetadata put(String compositeName, String ingestType, String field) {
        addCompositeMetadata(compositeName, ingestType, field);
        return this;
    }
    
    private void addCompositeMetadata(String compositeName, String ingestType, Collection<String> fields) {
        this.ingestTypes.add(ingestType);
        this.compositeMetadata.getUnchecked(compositeName).putAll(ingestType, fields);
    }
    
    public Multimap<String,String> get(String key) {
        return this.compositeMetadata.getUnchecked(key);
    }
    
    public boolean containsKey(String key) {
        return this.compositeMetadata.asMap().containsKey(key);
    }
    
    private void addCompositeMetadata(String compositeNameAndIndex, String ingestType, String fieldName) {
        this.ingestTypes.add(ingestType);
        log.debug("compositeMetadata:" + compositeMetadata.asMap());
        Multimap<String,String> multimap = this.compositeMetadata.getUnchecked(ingestType);
        multimap.put(fieldName, compositeNameAndIndex);
        log.debug("compositeMetadata:" + compositeMetadata.asMap());
    }
    
    public Collection<String> getCompositeMetadata(String fieldName, String ingestType) {
        return this.compositeMetadata.getUnchecked(fieldName).get(ingestType);
    }
    
    public Set<Entry<String,Multimap<String,String>>> entrySet() {
        return this.compositeMetadata.asMap().entrySet();
    }
    
    public Set<String> keySet() {
        return this.compositeMetadata.asMap().keySet();
    }
    
    public CompositeMetadata filter(Set<String> datatypeFilter) {
        if (datatypeFilter == null || datatypeFilter.isEmpty())
            return new CompositeMetadata(this);
        Map<String,Multimap<String,String>> localMap = Maps.newHashMap();
        for (String key : this.compositeMetadata.asMap().keySet()) {
            Multimap<String,String> mm = this.compositeMetadata.getUnchecked(key);
            Multimap<String,String> filtered = Multimaps.filterKeys(mm, Predicates.in(datatypeFilter));
            if (filtered.isEmpty() == false) {
                localMap.put(key, filtered);
            }
        }
        CompositeMetadata compositeMetadata = new CompositeMetadata();
        compositeMetadata.ingestTypes = datatypeFilter;
        compositeMetadata.compositeMetadata.asMap().putAll(localMap);
        return compositeMetadata;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        for (String ingType : compositeMetadata.asMap().keySet()) {
            if (sb.length() > 0) {
                sb.append(';');
            }
            
            sb.append(ingType).append(':');
            sb.append('[');
            boolean firstField = true;
            for (String fieldName : compositeMetadata.getUnchecked(ingType).keySet()) {
                if (!firstField)
                    sb.append(';');
                firstField = false;
                sb.append(fieldName);
                sb.append(':');
                boolean first = true;
                for (String compositeNameAndIndex : compositeMetadata.getUnchecked(ingType).get(fieldName)) {
                    compositeNameAndIndex = bracketCompositeNameAndIndex(compositeNameAndIndex);
                    if (!first)
                        sb.append(',');
                    sb.append(compositeNameAndIndex);
                    first = false;
                }
            }
            sb.append(']');
            log.trace("sb:" + sb);
        }
        return sb.toString();
    }
    
    public Map<String,Multimap<String,String>> getCompositeToFieldMap() {
        Pattern fieldAndIndexPattern = Pattern.compile("(.*)\\[(\\d*)\\]");
        
        for (String ingestType : this.compositeMetadata.asMap().keySet()) {
            Multimap<String,String> mm = this.compositeMetadata.getUnchecked(ingestType);
            for (String fieldName : mm.asMap().keySet()) {
                Collection<String> list = mm.get(fieldName);
                for (String compositeNameAndIndex : list) {
                    if (compositeNameAndIndex.indexOf(',') != -1) {
                        String[] splits = Iterables.toArray(Splitter.on(',').omitEmptyStrings().trimResults().split(compositeNameAndIndex), String.class);
                        if (splits.length == 2) {
                            int idx = Integer.parseInt(splits[1]);
                            String compositeName = splits[0];
                            Multimap<String,String> ctfmm = this.compositeToFieldMap.getUnchecked(ingestType);
                            List<String> gotList = (List<String>) ctfmm.get(compositeName);
                            if (gotList.size() < idx) {
                                idx = gotList.size();
                            }
                            gotList.add(idx, fieldName);
                        }
                    } else {
                        Matcher matcher = fieldAndIndexPattern.matcher(compositeNameAndIndex);
                        if (matcher.matches()) {
                            String compositeName = matcher.group(1);
                            int idx = Integer.parseInt(matcher.group(2));
                            Multimap<String,String> ctfmm = this.compositeToFieldMap.getUnchecked(ingestType);
                            List<String> gotList = (List<String>) ctfmm.get(compositeName);
                            if (gotList.size() < idx) {
                                idx = gotList.size();
                            }
                            gotList.add(idx, fieldName);
                        }
                    }
                }
            }
        }
        return this.compositeToFieldMap.asMap();
    }
    
    private String bracketCompositeNameAndIndex(String in) {
        StringBuilder buf = new StringBuilder();
        if (in.indexOf(',') != -1) {
            buf.append(in.replace(',', '[')).append(']');
        } else {
            buf.append(in);
        }
        return buf.toString();
    }
    
    private String unbracketCompositeNameAndIndex(String in) {
        StringBuilder buf = new StringBuilder();
        if (in.indexOf('[') != -1) {
            buf.append(in.replace('[', ',').replaceAll("\\]", ""));
        } else {
            buf.append(in);
        }
        return buf.toString();
    }
    
    public String toString(Set<String> ingesttypeFilter) {
        StringBuilder sb = new StringBuilder();
        
        for (String fieldName : compositeMetadata.asMap().keySet()) {
            if (sb.length() > 0) {
                sb.append(';');
            }
            
            sb.append(fieldName).append(':');
            sb.append('[');
            boolean firstField = true;
            for (String ingestType : compositeMetadata.getUnchecked(fieldName).keySet()) {
                if (ingesttypeFilter.contains(ingestType)) {
                    if (!firstField)
                        sb.append(';');
                    firstField = false;
                    sb.append(ingestType);
                    sb.append(':');
                    boolean first = true;
                    for (String type : compositeMetadata.getUnchecked(fieldName).get(ingestType)) {
                        if (!first)
                            sb.append(',');
                        sb.append(type);
                        first = false;
                    }
                }
            }
            sb.append(']');
            log.trace("sb:" + sb);
        }
        return sb.toString();
    }
    
    private void fromString(String data) {
        log.debug("fromString input data:" + data);
        // was:
        // field1:a,b;field2:d,e;field3:y,z
        // test:[MAKE_COLOR:MAKE,COLOR;COLOR_WHEELS:COLOR,WHEELS];beep:[MAKE_COLOR:MAKE,COLOR;COLOR_WHEELS:COLOR,WHEELS];work:[MAKE_COLOR:MAKE,COLOR;COLOR_WHEELS:COLOR,WHEELS]
        // String should look like this:
        // field1:[type1:a,b;type2:b];field2:[type1:a,b;type2:a,c]
        
        String[] entries = parse(data, ';');
        for (String entry : entries) {
            String[] entrySplits = parse(entry, ':');
            if (2 != entrySplits.length) {
                // Do nothing
            } else {
                // entrySplits[1] looks like this:
                // [type1:a,b;type2:b] - split it on the ';'
                // get rid of the leading and trailing brackets:
                entrySplits[1] = entrySplits[1].substring(1, entrySplits[1].length() - 1);
                String[] values = parse(entrySplits[1], ';');
                Multimap<String,String> mm = HashMultimap.create();
                for (String value : values) {
                    String[] vs = Iterables.toArray(Splitter.on(':').omitEmptyStrings().trimResults().split(value), String.class);
                    String[] rhs = Iterables.toArray(Splitter.on(',').omitEmptyStrings().trimResults().split(vs[1]), String.class);
                    this.ingestTypes.add(vs[0]);
                    for (String r : rhs) {
                        mm.put(vs[0], r);
                    }
                }
                compositeMetadata.put(entrySplits[0], mm);
            }
        }
        log.debug("asMap:" + this.compositeMetadata.asMap());
        log.debug("made:" + this);
    }
    
    /**
     * split 'in' on the provided 'c'. ignore 'c' that is within a pair of square brackets [] for c == ';'
     * "field1:[type1:a,b;type2:b];field2:[type1:a,b;type2:a,c]" becomes: { "field1:[type1:a,b;type2:b]", "field2:[type1:a,b;type2:a,c]" }
     * 
     * @param in
     * @param c
     * @return
     */
    private static String[] parse(String in, char c) {
        List<String> list = Lists.newArrayList();
        int inside = 0;
        int start = 0;
        for (int i = 0; i < in.length(); i++) {
            if (in.charAt(i) == '[')
                inside++;
            if (in.charAt(i) == ']')
                inside--;
            if (in.charAt(i) == c && inside == 0) {
                list.add(in.substring(start, i));
                start = i + 1;
            }
        }
        list.add(in.substring(start));
        return Iterables.toArray(list, String.class);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((compositeMetadata == null) ? 0 : compositeMetadata.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CompositeMetadata other = (CompositeMetadata) obj;
        if (compositeMetadata == null) {
            if (other.compositeMetadata != null)
                return false;
        } else if (!compositeMetadata.asMap().equals(other.compositeMetadata.asMap()))
            return false;
        return true;
    }
}
