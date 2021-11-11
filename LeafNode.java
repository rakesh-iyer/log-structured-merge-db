import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Getter @Setter
public class LeafNode extends Node {
    int maxSize;
    static final int LEAF_NODE_IDENTIFIER = 2;
    static final int MAX_KEYS = 20;
    DirectoryNode parent;

    List<KeyData> keyDataList = new ArrayList<>();

    LeafNode(DirectoryNode parent) {
        this.maxSize = MAX_KEYS;
        this.parent = parent;
    }

    String getStartKey() {
        return keyDataList.get(0).getKey();
    }

    String getEndKey() {
        return keyDataList.get(keyDataList.size() - 1).getKey();
    }

    void add(KeyData keyData) {
        keyDataList.add(keyData);
    }

    boolean isFull() {
        return keyDataList.size() == maxSize;
    }

    void serialize(ByteBuffer bb) throws IOException {
        bb.putInt(LEAF_NODE_IDENTIFIER);
        bb.putInt(keyDataList.size());
        for (KeyData keyData: keyDataList) {
            keyData.serialize(bb);
        }
    }

    static LeafNode deserialize(ByteBuffer bb, DirectoryNode parent) throws Exception {
        LeafNode leafNode = new LeafNode(parent);

        if (bb.getInt() != LEAF_NODE_IDENTIFIER) {
            throw new Exception("Expecting Leaf but found wrong node type.");
        }

        int numKeys = bb.getInt();
        for (int i = 0; i < numKeys; i++) {
            KeyData nextKeyData = KeyData.deserialize(bb);
            leafNode.keyDataList.add(nextKeyData);
        }

        return leafNode;
    }

    String search(String key) {
        // you could do binary search here.
        for (KeyData keyData: keyDataList) {
            if (keyData.getKey().equals(key)) {
                return keyData.getData();
            }
        }

        return null;
    }

    void writeToMultiPageBlock(MultiPageBlock multiPageBlock, int pageOffset) throws Exception {
        ByteBuffer bb = multiPageBlock.getPageBuffer(pageOffset);

        serialize(bb);
    }
}
