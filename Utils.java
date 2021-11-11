import java.nio.ByteBuffer;

public class Utils {

    static byte getByte(boolean b) {
        return b ? (byte)1 : 0;
    }

    static boolean getBoolean(byte b) {
        return b == 1 ? true : false;
    }

    static void serializeString(ByteBuffer bb, String str) {
        bb.putInt(str.length());
        bb.put(str.getBytes());
    }

    static String deserializeString(ByteBuffer bb) {
        byte[] byteArray = new byte[bb.getInt()];
        bb.get(byteArray);
        return new String(byteArray);
    }
}
