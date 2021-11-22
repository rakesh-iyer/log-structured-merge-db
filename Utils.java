import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


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

    static <T extends Comparable> List<T> merge(List<T> list1, List<T> list2) {
        if (list1.isEmpty()) {
            return list2;
        } else if (list2.isEmpty()) {
            return list1;
        }

        List<T> merged = new ArrayList<>();
        Iterator<T> iter1 = list1.iterator();
        Iterator<T> iter2 = list2.iterator();

        T next1 = iter1.next();
        T next2 = iter2.next();

        do {
            // we will add timestamp comparisions later.
            if (next1.compareTo(next2) >= 0) {
                merged.add(next1);
                next1 = iter1.hasNext() ? iter1.next() : null;
            } else {
                merged.add(next2);
                next2 = iter2.hasNext() ? iter2.next() : null;
            }
        } while (next1 != null && next2 != null);

        // add the remaining items to the merged list;
        if (next2 == null) {
            merged.add(next1);
            while (iter1.hasNext()) {
                merged.add(iter1.next());
            }
        } else {
            merged.add(next2);
            while (iter2.hasNext()) {
                merged.add(iter2.next());
            }
        }

        return merged;
    }

    static byte[] readFile(String fileName, byte[] buffer) throws Exception {
        FileInputStream file = new FileInputStream(new File(fileName));

        file.read(buffer);

        return buffer;
    }

    static byte[] readFile(String fileName, byte[] buffer, int offset, int length) throws Exception {
        FileInputStream file = new FileInputStream(new File(fileName));

        file.read(buffer, offset, length);

        return buffer;
    }
}
