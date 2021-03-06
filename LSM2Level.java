import org.apache.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LSM2Level {
    static Logger logger = Logger.getLogger(LSM2Level.class);
    MemoryComponent memoryComponent = MemoryComponent.getInstance();
    static LevelMerge levelMerge = new LevelMerge();
    static final int MAX_ITERATIONS = 1;
    static final int KEYS_PER_ITERATION = 101;

    LSM2Level() {
        if (!Files.exists(Paths.get(DirectoryNode.ROOT_FILE_NAME))) {
            DirectoryNode.setRoot(new DirectoryNode(null));
            DirectoryNode.writeRoot();
        } else {
            DirectoryNode.readRoot();
        }
    }

    void insert(String key, String data) {
        memoryComponent.insert(key, data);
    }

/*    boolean delete(String key) {
        if (c0Component.remove(key) == null) {
            return c1Component.delete(key);
        }

        return true;
    }*/

    String search(String key) {
        String data = memoryComponent.search(key);

        if (data != null) {
            return data;
        }

        return DirectoryNode.getRoot().search(key);
    }

    List<KeyData> rangeSearch(String startKey, String endKey) throws Exception {
        List<KeyData> rangeListMemory = memoryComponent.rangeSearch(startKey, endKey);

        // query component 1.
        List<KeyData> rangeList = DirectoryNode.getRoot().rangeSearch(startKey, endKey);

        // merge the results.
        return Utils.merge(rangeListMemory, rangeList);
    }

    void inorder() throws Exception {
        DirectoryNode.getRoot().inorder();
    }

    void insertData() throws Exception {
        String key = "key";
        String data = "data";
        for (int iterations = 0, startIndex = 0; iterations < MAX_ITERATIONS; iterations++) {
            for (int j = 0 ; j < KEYS_PER_ITERATION; j++, startIndex++) {
                logger.debug("Inserting " + (key + startIndex));
                insert(key + startIndex, data + startIndex);
            }
        }

        Thread.sleep(10000);
    }

    void searchData() {
        String key = "key";
        String data = "data";
        for (int iterations = 0, startIndex = 0; iterations < MAX_ITERATIONS; iterations++) {
            int keysFound = 0;
            for (int j = 0 ; j < KEYS_PER_ITERATION; j++, startIndex++) {
                String result = search(key + startIndex);
                if (result == null || !result.equals(data + startIndex)) {
                    logger.info("Did not find the key " + (key + startIndex) + ":" + result);
                } else {
                    keysFound++;
                }
            }

            if (keysFound == KEYS_PER_ITERATION) {
                logger.info("This was a successful iteration.");
            } else {
                logger.info(String.format("Found %s keys.", keysFound));
            }
        }
    }

    void rangeSearchData() throws Exception {
        List<KeyData> keyDataList = rangeSearch("key0", "key500");
        for (KeyData keyData: keyDataList) {
            logger.info("Key:: " + keyData.getKey() + ":" + keyData.getData());
        }
    }

    public static void main(String args[]) throws Exception {
        boolean rangeSearch = true;
        LSM2Level lsm = new LSM2Level();
        levelMerge.start();

        lsm.insertData();

        levelMerge.stopMerge();

        Thread.sleep(10000);

//        lsm.inorder();

        if (rangeSearch) {
            lsm.rangeSearchData();
        } else {
            lsm.searchData();
        }
    }
}
