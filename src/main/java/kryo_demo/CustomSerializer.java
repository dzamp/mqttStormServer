package kryo_demo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;

public class CustomSerializer extends Serializer<FusionValues> implements Serializable {

    @Override
    public void write(Kryo kryo, Output output, FusionValues fusionValues) {
        output.writeString(fusionValues.id);
        output.writeString(fusionValues.topic);
        output.writeInt(fusionValues.value);
        output.writeLong(fusionValues.timestamp);
    }

    @Override
    public FusionValues read(Kryo kryo, Input input, Class<FusionValues> aClass) {
        return new FusionValues(input.readString(),input.readInt(),input.readString(), input.readLong());
    }
}
