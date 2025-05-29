package datawave.data.type;

import datawave.data.normalizer.Normalizer;

public class NumberListType extends ListType {

    public NumberListType() {
        super(Normalizer.NUMBER_NORMALIZER);
    }

    public NumberListType(String delegateString) {
        super(delegateString, Normalizer.NUMBER_NORMALIZER);
    }

    @Override
    public boolean expandAtQueryTime() {
        return true;
    }
}
