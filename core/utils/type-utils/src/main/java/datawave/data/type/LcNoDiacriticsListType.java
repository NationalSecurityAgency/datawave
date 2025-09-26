package datawave.data.type;

import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.normalizer.Normalizer;

public class LcNoDiacriticsListType extends ListType {

    public LcNoDiacriticsListType() {
        super(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
    }

    public LcNoDiacriticsListType(String delegateString) {
        super(delegateString, Normalizer.LC_NO_DIACRITICS_NORMALIZER);
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
        this.delegate = input.readString();
        int size = input.readInt(true);
        normalizedValues = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            normalizedValues.add(input.readString());
        }
    }

}
