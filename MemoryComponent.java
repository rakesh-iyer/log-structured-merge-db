import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryComponent {
    static Logger logger = Logger.getLogger(MemoryComponent.class);
    // keeping this simple, ideally
    NavigableMap<String, String> dataMap = new ConcurrentSkipListMap<>();
    static MemoryComponent memoryComponentInstance;
    // Lets keep threshold fixed for now.
    static final int MEMORY_THRESHOLD = 0;
    private MemoryComponent() {
    }

    synchronized static MemoryComponent getInstance() {
        if (memoryComponentInstance == null) {
            memoryComponentInstance = new MemoryComponent();
        }
        return memoryComponentInstance;
    }

    boolean exceedsThreshold() {
        return dataMap.size() > MEMORY_THRESHOLD;
    }

    List<KeyData> removeEntries(int maxEntries) {
        List<KeyData> removedBatch = new ArrayList<>();

        for (Map.Entry<String, String> entry: dataMap.entrySet()) {
            removedBatch.add(new KeyData(entry.getKey(), entry.getValue()));
            dataMap.remove(entry.getKey());

            if (removedBatch.size() >= maxEntries) {
                break;
            }
        }

        return removedBatch;
    }

    List<KeyData> removeKeyDataForMerge(String startKey, String endKey) {
        // startKey if non null is non inclusive as it is the previous node's end key and so is already in a valid range.
        boolean startKeyInclusive = false;

        // Handle special cases.
        if (startKey == null) {
            startKey = dataMap.firstKey();
            if (endKey != null && startKey.compareTo(endKey) > 0) {
                return new ArrayList<>();
            }
            startKeyInclusive = true;
        }
        if (endKey == null) {
            endKey = dataMap.lastKey();
            if (startKey != null && startKey.compareTo(endKey) > 0) {
                return new ArrayList<>();
            }
        }

        logger.debug(startKey);
        logger.debug(endKey);
        Map<String, String> removedMap = dataMap.subMap(startKey, startKeyInclusive, endKey, true);
        List<KeyData> removedBatch = new ArrayList<>();

        for (Map.Entry<String, String> entry: removedMap.entrySet()) {
            removedBatch.add(new KeyData(entry.getKey(), entry.getValue()));
            dataMap.remove(entry.getKey());
        }

        return removedBatch;
    }

    List<KeyData> rangeSearch(String startKey, String endKey) {
        // startKey if non null is non inclusive as it is the previous node's end key and so is already in a valid range.
        boolean startKeyInclusive = false;

        // Handle special cases.
        if (startKey == null) {
            startKey = dataMap.firstKey();
            if (endKey != null && startKey.compareTo(endKey) > 0) {
                return new ArrayList<>();
            }
            startKeyInclusive = true;
        }
        if (endKey == null) {
            endKey = dataMap.lastKey();
            if (startKey != null && startKey.compareTo(endKey) > 0) {
                return new ArrayList<>();
            }
        }

        logger.debug(startKey);
        logger.debug(endKey);
        Map<String, String> rangeMap = dataMap.subMap(startKey, startKeyInclusive, endKey, true);
        List<KeyData> rangeResult = new ArrayList<>();

        for (Map.Entry<String, String> entry: rangeMap.entrySet()) {
            rangeResult.add(new KeyData(entry.getKey(), entry.getValue()));
        }

        return rangeResult;
    }

    void insert(String key, String data) {
        dataMap.put(key, data);
    }

    String search(String key) {
        return dataMap.get(key);
    }
}
