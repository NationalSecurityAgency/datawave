package datawave.query.cardinality;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import datawave.common.cl.OptionBuilder;
import datawave.security.util.ScannerHelper;
import datawave.util.cli.PasswordConverter;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

public class CardinalityScanner {
    
    private static final String ZOOKEEPERS = "zookeepers";
    private static final String INSTANCE = "instance";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String AUTHS = "auths";
    
    private static final String TABLE = "table";
    private static final String D_OPT = "date";
    private static final String F_OPT = "field";
    private static final String SORTBYCARDINALITY = "sortByCardinality";
    private static final String AGGREGATE = "aggregate";
    private static final String DATATYPES = "datatypes";
    private static final String INTERSECT = "intersect";
    
    private static final String HELP_OPT = "help";
    
    private static final Logger log = Logger.getLogger(CardinalityScanner.class);
    private CardinalityScannerConfiguration config = null;
    
    public enum DateAggregationType {
        DAY, MONTH, ALL
    }
    
    public enum DatatypeAggregationType {
        USE, IGNORE
    }
    
    public static void main(String[] args) {
        
        Logger.getRootLogger().setLevel(Level.ERROR);
        
        Options opts = getConfigurationOptions();
        CommandLine cl = null;
        try {
            cl = new DefaultParser().parse(opts, args);
            if (cl.hasOption(HELP_OPT)) {
                new HelpFormatter().printHelp(CardinalityScanner.class.getName() + ":", opts, true);
                return;
            }
        } catch (ParseException pe) {
            System.out.println(pe.getMessage());
            new HelpFormatter().printHelp(CardinalityScanner.class.getName() + ":", opts, true);
            return;
        }
        
        try {
            CardinalityScannerConfiguration config = getConfiguration(cl);
            CardinalityScanner cardinalityScanner = new CardinalityScanner(config);
            
            DatatypeAggregationType datatypeAggregation = DatatypeAggregationType.IGNORE;
            if (config.getMaintainDatatypes() == true) {
                datatypeAggregation = DatatypeAggregationType.USE;
            }
            
            Set<CardinalityIntersectionRecord> cardinalitySet = cardinalityScanner.scanCardinalities(config.getFields(), config.getDateAggregateMode(),
                            datatypeAggregation);
            System.out.println("DATE" + "," + "DATATYPE" + "," + "PAIR" + "," + "CARDINALITY");
            
            if (config.getIntersect()) {
                if (config.getFields().size() == 2) {
                    
                    Set<String> dates = new TreeSet<>();
                    Set<String> datatypes = new TreeSet<>();
                    for (CardinalityIntersectionRecord c : cardinalitySet) {
                        dates.add(c.getDate());
                        datatypes.add(c.getDatatype());
                    }
                    
                    for (String currDate : dates) {
                        for (String currDatatype : datatypes) {
                            Set<CardinalityIntersectionRecord> intersectionSet = cardinalityScanner.intersect(cardinalitySet, currDate, currDatatype,
                                            config.getFields());
                            cardinalityScanner.printIntersectionRecords(intersectionSet);
                        }
                    }
                } else {
                    throw new IllegalArgumentException("must have exactly two fields to intersect cardinalities");
                }
            } else {
                cardinalityScanner.printCardinalityRecords(cardinalitySet);
            }
            
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public static Options getConfigurationOptions() {
        
        final OptionBuilder builder = new OptionBuilder();
        final Options opt = new Options();
        
        opt.addOption(builder.create(HELP_OPT, null, "show help"));
        
        builder.setArgs(1);
        builder.type = String.class;
        builder.setRequired(true);
        
        opt.addOption(builder.create(ZOOKEEPERS, null, "list of Zookeepers host[:port],host[:port]"));
        opt.addOption(builder.create(INSTANCE, null, "accumulo instance name"));
        opt.addOption(builder.create(USERNAME, null, "accumulo user name"));
        opt.addOption(builder.create(PASSWORD, null, "accumulo password"));
        opt.addOption(builder.create(AUTHS, null, "authorizations"));
        
        opt.addOption(builder.create(TABLE, null, "read records from this accumulo table"));
        opt.addOption(builder.create(D_OPT, null, "date or date range (yyyyMMdd, yyyyMMdd-yyyyMMdd)"));
        
        builder.setRequired(false);
        opt.addOption(builder.create(F_OPT, null, "field(s)"));
        opt.addOption(builder.create(AGGREGATE, null, "aggregate cardinalities DAY|MONTH|ALL (default DAY)"));
        
        builder.type = Boolean.class;
        builder.setArgs(0);
        opt.addOption(builder.create(DATATYPES, null, "maintain datatypes when reading cardinalities"));
        opt.addOption(builder.create(INTERSECT, null, "intersect cardinalities"));
        opt.addOption(builder.create(SORTBYCARDINALITY, null, "sort by cardinality within pairs"));
        
        return opt;
    }
    
    public static CardinalityScannerConfiguration getConfiguration(CommandLine cl) throws Exception {
        
        CardinalityScannerConfiguration config = new CardinalityScannerConfiguration();
        config.setZookeepers(cl.getOptionValue(ZOOKEEPERS));
        config.setInstanceName(cl.getOptionValue(INSTANCE));
        config.setUsername(cl.getOptionValue(USERNAME));
        config.setPassword(PasswordConverter.parseArg(cl.getOptionValue(PASSWORD)));
        config.setTableName(cl.getOptionValue(TABLE));
        config.setAuths(cl.getOptionValue(AUTHS));
        try {
            config.setDateAggregateMode(DateAggregationType.valueOf(cl.getOptionValue(AGGREGATE, "DAY").toUpperCase()));
        } catch (Exception e) {
            // do nothing
        }
        config.setMaintainDatatypes(cl.hasOption(DATATYPES));
        config.setIntersect(cl.hasOption(INTERSECT));
        config.setSortByCardinality(cl.hasOption(SORTBYCARDINALITY));
        
        String dateOpt = cl.getOptionValue(D_OPT);
        if (dateOpt != null) {
            if (dateOpt.indexOf("-") != -1) {
                String[] splits = StringUtils.split(dateOpt, '-');
                config.setBeginDate(splits[0].trim());
                config.setEndDate(splits[1].trim());
            } else {
                config.setBeginDate(dateOpt.trim());
                config.setEndDate(dateOpt.trim());
            }
        }
        List<String> fields = new ArrayList<>();
        String[] fieldArray = cl.getOptionValues(F_OPT);
        if (fieldArray != null) {
            Collections.addAll(fields, fieldArray);
        }
        config.setFields(fields);
        return config;
    }
    
    public CardinalityScanner(CardinalityScannerConfiguration config) {
        
        this.config = config;
    }
    
    public Set<CardinalityIntersectionRecord> scanCardinalities(List<String> fields, DateAggregationType dateAggregationType,
                    DatatypeAggregationType datatypeAggregationType) throws Exception {
        
        Map<CardinalityIntersectionRecord,HyperLogLogPlus> cardinalityMap = new TreeMap<>();
        try (AccumuloClient client = Accumulo.newClient().to(config.getInstanceName(), config.getZookeepers()).as(config.getUsername(), config.getPassword())
                        .build()) {
            Collection<Authorizations> authCollection = Collections.singleton(new Authorizations(config.getAuths().split(",")));
            if (!client.tableOperations().exists(config.getTableName())) {
                throw new IllegalArgumentException("Table " + config.getTableName() + " does not exist");
            }
            try (Scanner scanner = ScannerHelper.createScanner(client, config.getTableName(), authCollection)) {
                Range r = new Range(config.getBeginDate(), config.getEndDate() + "\0");
                scanner.setRange(r);
                
                Iterator<Map.Entry<Key,Value>> itr = scanner.iterator();
                while (itr.hasNext()) {
                    Map.Entry<Key,Value> nextEntry = itr.next();
                    Key key = nextEntry.getKey();
                    String field = key.getColumnFamily().toString();
                    if (fields != null && !fields.isEmpty() && !fields.contains(field)) {
                        continue;
                    } else {
                        addEntry(cardinalityMap, nextEntry, dateAggregationType, datatypeAggregationType);
                    }
                }
            }
        } catch (Exception e) {
            log.error(e);
        }
        return cardinalityMap.keySet();
    }
    
    private void printCardinalityRecords(Set<CardinalityIntersectionRecord> cardinalitySet) {
        
        if (config.getSortByCardinality()) {
            
            TreeMap<Long,TreeSet<CardinalityIntersectionRecord>> orderByCardinality = new TreeMap<>();
            for (CardinalityIntersectionRecord cardinalityIntersectionRecord : cardinalitySet) {
                Long cardinality = cardinalityIntersectionRecord.getBaseHllp().cardinality();
                if (cardinality > 0) {
                    TreeSet<CardinalityIntersectionRecord> set = orderByCardinality.get(cardinality);
                    if (set == null) {
                        set = new TreeSet<>();
                        orderByCardinality.put(cardinality, set);
                    }
                    set.add(cardinalityIntersectionRecord);
                }
            }
            
            for (Map.Entry<Long,TreeSet<CardinalityIntersectionRecord>> entry : orderByCardinality.descendingMap().entrySet()) {
                for (CardinalityIntersectionRecord cardinalityType : entry.getValue()) {
                    System.out.println(cardinalityType.getDate() + "," + cardinalityType.getDatatype() + "," + cardinalityType.getFieldName() + "/"
                                    + cardinalityType.getFieldValue() + "," + entry.getKey());
                }
            }
        } else {
            for (CardinalityIntersectionRecord cardinalityIntersectionRecord : cardinalitySet) {
                System.out.println(cardinalityIntersectionRecord.getDate() + "," + cardinalityIntersectionRecord.getDatatype() + ","
                                + cardinalityIntersectionRecord.getFieldName() + "/" + cardinalityIntersectionRecord.getFieldValue() + ","
                                + cardinalityIntersectionRecord.getBaseHllp().cardinality());
            }
        }
    }
    
    private void printIntersectionRecords(Set<CardinalityIntersectionRecord> cardinalitySet) {
        
        for (CardinalityIntersectionRecord intersectionRecord : cardinalitySet) {
            String field1 = intersectionRecord.tuple.getValue0();
            String value1 = intersectionRecord.tuple.getValue1();
            String currDate = intersectionRecord.getDate();
            String currDatatype = intersectionRecord.getDatatype();
            
            Map<Pair<String,String>,Long> intersectionMap = intersectionRecord.getIntersectionMap();
            
            if (config.getSortByCardinality()) {
                TreeMap<Long,TreeSet<Pair<String,String>>> orderByCardinality = new TreeMap<>();
                
                for (Map.Entry<Pair<String,String>,Long> entry : intersectionMap.entrySet()) {
                    if (entry.getValue() > 0) {
                        TreeSet<Pair<String,String>> set = orderByCardinality.get(entry.getValue());
                        if (set == null) {
                            set = new TreeSet<>();
                            orderByCardinality.put(entry.getValue(), set);
                        }
                        set.add(entry.getKey());
                    }
                }
                
                for (Map.Entry<Long,TreeSet<Pair<String,String>>> entry : orderByCardinality.descendingMap().entrySet()) {
                    for (Pair<String,String> pair : entry.getValue()) {
                        System.out.println(currDate + "," + currDatatype + "," + field1 + "/" + value1 + "/" + pair.getValue0() + "/" + pair.getValue1() + ","
                                        + entry.getKey());
                    }
                }
            } else {
                for (Map.Entry<Pair<String,String>,Long> entry : intersectionMap.entrySet()) {
                    if (entry.getValue() > 0) {
                        System.out.println(currDate + "," + currDatatype + "," + field1 + "/" + value1 + "/" + entry.getKey().getValue0() + "/"
                                        + entry.getKey().getValue1() + "," + entry.getValue());
                    }
                }
            }
        }
    }
    
    private Set<CardinalityIntersectionRecord> intersect(Set<CardinalityIntersectionRecord> cardinalityMap, String date, String datatype, List<String> fields) {
        
        Set<CardinalityIntersectionRecord> result = new TreeSet<>();
        try {
            String field1 = fields.get(0);
            String field2 = fields.get(1);
            
            Map<String,CardinalityIntersectionRecord> cIntersectionMap = new TreeMap<>();
            
            // create CardinalityIntersectionRecord for every value of field1
            for (CardinalityIntersectionRecord cardinalityIntersectionRecord : cardinalityMap) {
                if (cardinalityIntersectionRecord.getDate().equals(date) && cardinalityIntersectionRecord.getDatatype().equals(datatype)) {
                    String currFieldName = cardinalityIntersectionRecord.getFieldName();
                    String currFieldValue = cardinalityIntersectionRecord.getFieldValue();
                    
                    String s = field1 + ":" + currFieldValue;
                    CardinalityIntersectionRecord cIntersection = cIntersectionMap.get(s);
                    if (currFieldName.equals(field1)) {
                        if (cIntersection == null) {
                            cIntersection = new CardinalityIntersectionRecord(date, datatype, field1, currFieldValue);
                            cIntersection.setBaseHllp(cardinalityIntersectionRecord.getBaseHllp());
                            cIntersectionMap.put(s, cIntersection);
                        }
                    }
                }
            }
            
            // intersect every value of field2 with
            for (CardinalityIntersectionRecord cardinalityIntersectionRecord : cardinalityMap) {
                if (cardinalityIntersectionRecord.getDate().equals(date) && cardinalityIntersectionRecord.datatype.equals(datatype)) {
                    String currFieldName = cardinalityIntersectionRecord.getFieldName();
                    String currFieldValue = cardinalityIntersectionRecord.getFieldValue();
                    if (currFieldName.equals(field2)) {
                        for (CardinalityIntersectionRecord cIntersection : cIntersectionMap.values()) {
                            cIntersection.addPair(currFieldName, currFieldValue, cardinalityIntersectionRecord.getBaseHllp());
                        }
                    }
                }
            }
            
            result.addAll(cIntersectionMap.values());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return result;
    }
    
    protected void addEntry(Map<CardinalityIntersectionRecord,HyperLogLogPlus> cardinalityMap, Map.Entry<Key,Value> entry,
                    DateAggregationType dateAggregationType, DatatypeAggregationType datatypeAggregationType) throws IOException, CardinalityMergeException {
        
        Key key = entry.getKey();
        String colQual = key.getColumnQualifier().toString();
        
        String date = key.getRow().toString();
        switch (dateAggregationType) {
            case ALL:
                date = config.getBeginDate() + "-" + config.getEndDate();
                break;
            case MONTH:
                // remove the day portion of the date
                date = date.substring(0, 6);
                break;
            case DAY:
            default:
                // leave date as is
        }
        
        String fieldName = key.getColumnFamily().toString();
        String dataType = colQual.split("\0")[0];
        if (datatypeAggregationType.equals(DatatypeAggregationType.IGNORE)) {
            dataType = "ALL";
        }
        
        String fieldValue = colQual.split("\0")[1];
        HyperLogLogPlus hllpNew = HyperLogLogPlus.Builder.build(entry.getValue().get());
        
        // System.out.println(key.toString() + " -- " + hllpNew.cardinality());
        
        CardinalityIntersectionRecord card = new CardinalityIntersectionRecord(date, dataType, fieldName, fieldValue);
        
        HyperLogLogPlus hllpStored = cardinalityMap.get(card);
        if (hllpStored == null) {
            hllpStored = hllpNew;
        } else {
            hllpStored.addAll(hllpNew);
        }
        card.setBaseHllp(hllpStored);
        cardinalityMap.put(card, hllpStored);
    }
    
    public class CardinalityIntersectionRecord implements Comparable {
        
        public Pair<String,String> tuple = null;
        private HyperLogLogPlus baseHllp = null;
        
        private String date = null;
        private String datatype;
        
        private Map<Pair<String,String>,Long> intersectionSum = new TreeMap<>();
        private Map<Pair<String,String>,HyperLogLogPlus> intersectionUnion = new TreeMap<>();
        
        public CardinalityIntersectionRecord(String date, String datatype, String fieldName1, String fieldValue1) {
            this.tuple = new Pair<>(fieldName1, fieldValue1);
            this.date = date;
            this.datatype = datatype;
        }
        
        public void addPair(String fieldName, String fieldValue, HyperLogLogPlus hllp) throws Exception {
            if (fieldName.equals(tuple.getValue0())) {
                return;
            }
            Pair<String,String> p = new Pair<>(fieldName, fieldValue);
            Long currSum = intersectionSum.get(p);
            HyperLogLogPlus currHllp = intersectionUnion.get(p);
            HyperLogLogPlus newHllp = HyperLogLogPlus.Builder.build(hllp.getBytes());
            if (currSum == null) {
                intersectionSum.put(p, baseHllp.cardinality() + hllp.cardinality());
                newHllp.addAll(baseHllp);
                intersectionUnion.put(p, newHllp);
            } else {
                intersectionSum.put(p, currSum + hllp.cardinality());
                newHllp.addAll(currHllp);
                intersectionUnion.put(p, newHllp);
            }
        }
        
        public Map<Pair<String,String>,Long> getIntersectionMap() {
            
            TreeMap<Pair<String,String>,Long> intersectionMap = new TreeMap<>();
            for (Pair<String,String> p : intersectionSum.keySet()) {
                long sum = intersectionSum.get(p);
                long union = intersectionUnion.get(p).cardinality();
                
                intersectionMap.put(p, Long.valueOf(sum - union));
            }
            return intersectionMap;
        }
        
        @Override
        public boolean equals(Object obj) {
            
            CardinalityIntersectionRecord other = null;
            if (obj instanceof CardinalityIntersectionRecord) {
                other = (CardinalityIntersectionRecord) obj;
            } else {
                return false;
            }
            EqualsBuilder builder = new EqualsBuilder();
            builder.append(this.date, other.date);
            builder.append(this.datatype, other.datatype);
            builder.append(this.tuple, other.tuple);
            return builder.isEquals();
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(tuple, date, datatype);
        }
        
        @Override
        public int compareTo(Object obj) {
            CardinalityIntersectionRecord other = null;
            if (obj instanceof CardinalityIntersectionRecord) {
                other = (CardinalityIntersectionRecord) obj;
            } else {
                throw new IllegalArgumentException(obj + " is not a CardinalityIntersection");
            }
            CompareToBuilder builder = new CompareToBuilder();
            builder.append(this.date, other.date);
            builder.append(this.datatype, other.datatype);
            builder.append(this.tuple, other.tuple);
            return builder.toComparison();
        }
        
        public String getDate() {
            return date;
        }
        
        public void setDate(String date) {
            this.date = date;
        }
        
        public String getDatatype() {
            return datatype;
        }
        
        public void setDatatype(String datatype) {
            this.datatype = datatype;
        }
        
        public String getFieldName() {
            return tuple.getValue0();
        }
        
        public String getFieldValue() {
            return tuple.getValue1();
        }
        
        public void setBaseHllp(HyperLogLogPlus baseHllp) {
            this.baseHllp = baseHllp;
        }
        
        public HyperLogLogPlus getBaseHllp() {
            return baseHllp;
        }
    }
}
