package datawave.age.off.util;

import java.util.HashMap;

public class FridgeToFreezerMapping extends HashMap<String,String> {
    private static final long serialVersionUID = 332498820763181265L;

    public FridgeToFreezerMapping() {
        super();
        this.put("1h", "0s");
        this.put("1d", "3d");
        this.put("2d", "7d");
        this.put("3d", "7d");
        this.put("4d", "10d");
        this.put("5d", "14d");
        this.put("7d", "30d");
        this.put("10d", "42d");
        this.put("35d", "365d");
        this.put("14d", "60d");
        this.put("90d", "365d");
        this.put("270d", "1095d");
        this.put("730d", "1826d");
    }

    @Override
    public String get(Object key) {
        String result = super.get(key);
        // If uncertain, discard
        if (null == result) {
            return "0s";
        }
        return result;
    }
}
