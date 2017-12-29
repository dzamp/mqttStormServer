package kafka_example;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableTable;
import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MyScheme implements KeyValueScheme {
    private static final Charset UTF8_CHARSET;
    public static final String STRING_SCHEME_KEY = "str";

    static {
        UTF8_CHARSET = StandardCharsets.UTF_8;
    }

    public static String deserializeString(ByteBuffer string) {
        if (string.hasArray()) {
            int base = string.arrayOffset();
            return new String(string.array(), base + string.position(), string.remaining());
        } else {
            return new String(Utils.toByteArray(string), UTF8_CHARSET);
        }
    }

    @Override
    public List<Object> deserializeKeyAndValue(ByteBuffer byteBuffer, ByteBuffer byteBuffer1) {
        String key = byteBuffer.toString();
        String value = byteBuffer1.toString();
        return ImmutableList.of(key, value);
    }

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        String val = new String(ser.asCharBuffer().array());
        String objects = new String(ser.array());
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(new String[]{"str"});
    }
}
