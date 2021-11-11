import java.nio.file.Files;
import java.nio.file.Paths;

public class LSM2Level {
    MemoryComponent memoryComponent = MemoryComponent.getInstance();
    DirectoryNode root;
    static LevelMerge levelMerge = new LevelMerge();

    LSM2Level() {
        if (Files.exists(Paths.get(DirectoryNode.ROOT_FILE_NAME))) {
            root = DirectoryNode.getRoot();
        } else {
            root = new DirectoryNode(null);
            DirectoryNode.setRoot(root);
            DirectoryNode.writeRoot();
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

        return root.search(key);
    }

    public static void main(String args[]) throws Exception {
        LSM2Level lsm = new LSM2Level();
        levelMerge.start();

        String key = "key";
        String data = "data";

        int i = 0;
        while (true) {
            for (int j = 0 ; j < 101; j++, i++) {
                lsm.insert(key + i, data + i);
            }
/*
            for (int j = 0; i < 101; i++) {
                System.out.println(lsm.search(key + i));
            }
*/
            Thread.sleep(2000);
        }

    }

}
