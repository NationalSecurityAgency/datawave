package datawave.ingest.mapreduce.handler.shard;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

import java.io.IOException;

public class FieldIndexData implements Message<FieldIndexData> {
    
    private FieldIndexFilterData filterData;
    private byte[] bloomFilterBytes;
    
    public FieldIndexData() {
        this(null, null);
    }
    
    public FieldIndexData(FieldIndexFilterData filterData, byte[] bloomFilterBytes) {
        this.filterData = filterData;
        this.bloomFilterBytes = bloomFilterBytes;
    }
    
    public FieldIndexFilterData getFilterData() {
        return filterData;
    }
    
    public void setFilterData(FieldIndexFilterData filterData) {
        this.filterData = filterData;
    }
    
    public byte[] getBloomFilterBytes() {
        return bloomFilterBytes;
    }
    
    public void setBloomFilterBytes(byte[] bloomFilterBytes) {
        this.bloomFilterBytes = bloomFilterBytes;
    }
    
    @Override
    public Schema<FieldIndexData> cachedSchema() {
        return SCHEMA;
    }
    
    public static Schema<FieldIndexData> SCHEMA = new Schema<FieldIndexData>() {
        
        public static final String FILTER_DATA = "filterData";
        public static final String BLOOM_FILTER_BYTES = "bloomFilterBytes";
        
        public Schema<FieldIndexFilterData> filterDataSchema = FieldIndexFilterData.SCHEMA;
        
        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return FILTER_DATA;
                case 2:
                    return BLOOM_FILTER_BYTES;
                default:
                    return null;
            }
        }
        
        @Override
        public int getFieldNumber(String name) {
            switch (name) {
                case FILTER_DATA:
                    return 1;
                case BLOOM_FILTER_BYTES:
                    return 2;
                default:
                    return 0;
            }
        }
        
        @Override
        public boolean isInitialized(FieldIndexData fieldIndexData) {
            return true;
        }
        
        @Override
        public FieldIndexData newMessage() {
            return new FieldIndexData();
        }
        
        @Override
        public String messageName() {
            return FieldIndexData.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return FieldIndexData.class.getName();
        }
        
        @Override
        public Class<? super FieldIndexData> typeClass() {
            return FieldIndexData.class;
        }
        
        @Override
        public void mergeFrom(Input input, FieldIndexData fieldIndexData) throws IOException {
            for (int number = input.readFieldNumber(this);; number = input.readFieldNumber(this)) {
                switch (number) {
                    case 0:
                        return;
                    case 1:
                        fieldIndexData.setFilterData(input.mergeObject(null, filterDataSchema));
                        break;
                    case 2:
                        fieldIndexData.setBloomFilterBytes(input.readString().getBytes("UTF8"));
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }
            }
        }
        
        @Override
        public void writeTo(Output output, FieldIndexData fieldIndexData) throws IOException {
            if (fieldIndexData.getFilterData() != null)
                output.writeObject(1, fieldIndexData.getFilterData(), filterDataSchema, false);
            if (fieldIndexData.getBloomFilterBytes() != null)
                output.writeString(2, new String(fieldIndexData.getBloomFilterBytes(), "UTF8"), false);
        }
    };
}
