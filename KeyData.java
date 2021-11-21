import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.ByteBuffer;

@Getter
@Setter
class KeyData implements Comparable<KeyData> {
    String key;
    String data;

    KeyData(String key, String data) {
        setKey(key);
        setData(data);
    }

    public int compareTo(KeyData other) {
        return key.compareTo(other.key);
    }

    static KeyData merge(KeyData keyData1, KeyData keyData2) {
        return keyData2;
    }

    void serialize(ByteBuffer bb) throws IOException  {
        bb.putInt(key.length());
        bb.put(key.getBytes());

        bb.putInt(data.length());
        bb.put(data.getBytes());
    }

    static KeyData deserialize(ByteBuffer bb) {
        byte[] newKeyBytes = new byte[bb.getInt()];
        bb.get(newKeyBytes);
        byte[] newDataBytes = new byte[bb.getInt()];
        bb.get(newDataBytes);

        return new KeyData(new String(newKeyBytes), new String(newDataBytes));
    }
}