package datawave.audit;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.IntRange;

import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;

public class SplitSelectorExtractor implements SelectorExtractor {

    protected String separatorCharacter = null;
    protected String separatorParameter = null;
    protected List<IntRange> useSplitsList = null;

    @Override
    public List<String> extractSelectors(Query query) throws IllegalArgumentException {
        List<String> selectorList = new ArrayList<>();

        String selectorSeparator = this.separatorCharacter;
        if (this.separatorParameter != null) {
            QueryImpl.Parameter parameter = query.findParameter(separatorParameter);
            if (parameter != null && parameter.getParameterValue() != null) {
                String value = parameter.getParameterValue();
                if (StringUtils.isNotBlank(value) && value.length() == 1) {
                    selectorSeparator = value;
                }
            }
        }

        String queryStr = query.getQuery();
        if (selectorSeparator == null) {
            selectorList.add(query.getQuery());
        } else {
            String[] querySplit = queryStr.split(selectorSeparator);
            for (int splitNumber = 0; splitNumber < querySplit.length; splitNumber++) {
                if (useSplitsList == null || useSplit(useSplitsList, splitNumber)) {
                    String s = querySplit[splitNumber];
                    selectorList.add(s.trim());
                }
            }
        }
        return selectorList;
    }

    public void setSeparatorCharacter(String separatorCharacter) {
        if (StringUtils.isNotBlank(separatorCharacter) && !separatorCharacter.isEmpty()) {
            this.separatorCharacter = separatorCharacter;
        } else {
            throw new RuntimeException("Illegal separator: '" + separatorCharacter + "'");
        }
    }

    public void setSeparatorParameter(String separatorParameter) {
        this.separatorParameter = separatorParameter;
    }

    public void setUseSplits(String useSplits) {

        this.useSplitsList = parseUseSplitsRanges(useSplits);
    }

    public List<IntRange> parseUseSplitsRanges(String useSplits) {

        List<IntRange> useSplitsList = new ArrayList<>();
        if (useSplits != null) {
            try {
                String[] split1 = useSplits.trim().split(",");
                for (String s : split1) {
                    String[] split2 = s.trim().split("-");
                    if (split2.length == 1) {
                        long v1 = Integer.parseInt(split2[0]);
                        if (s.endsWith("-")) {
                            useSplitsList.add(new IntRange(v1, Integer.MAX_VALUE));
                        } else {
                            useSplitsList.add(new IntRange(v1));
                        }
                    } else if (split2.length == 2) {
                        long v1 = Long.parseLong(split2[0]);
                        long v2 = Long.parseLong(split2[1]);
                        if (v1 > v2) {
                            throw new NumberFormatException(v1 + " > " + v2 + " in range " + s);
                        }
                        useSplitsList.add(new IntRange(v1, v2));
                    } else {
                        throw new NumberFormatException("range " + s + " contains more than 2 end points");
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return useSplitsList;
    }

    public boolean useSplit(List<IntRange> useSplitList, int splitNumber) {

        boolean useSplit = false;
        for (IntRange range : useSplitList) {
            if (range.containsInteger(splitNumber)) {
                useSplit = true;
                break;
            }
        }
        return useSplit;
    }
}
