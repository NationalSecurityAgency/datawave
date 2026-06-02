package datawave.data.type;

import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

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

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(getDelegateAsString());
        output.writeInt(normalizedValues.size(), true);
        for (String normalizedValue : normalizedValues) {
            output.writeString(normalizedValue);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        delegate = input.readString();

        normalizedValues = new ArrayList<>();
        int size = input.readInt(true);
        for (int i = 0; i < size; i++) {
            String normalizedValue = input.readString();
            normalizedValues.add(normalizedValue);
        }
    }
}
