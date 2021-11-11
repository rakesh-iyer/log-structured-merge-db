import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;

abstract public class Node extends Debuggable {
    static final int PAGE_SIZE = 4096;
    static Node read(MultiPageBlockHeader multiPageBlockHeader, int pageNumber, DirectoryNode parent) throws Exception {
        String multiPageBlockFileName =  multiPageBlockHeader.getMultiPageBlockNumber() + ".mpb";
        FileInputStream file = new FileInputStream(new File(multiPageBlockFileName));

        byte[] buffer = new byte[PAGE_SIZE];

        file.read(buffer, pageNumber * PAGE_SIZE, PAGE_SIZE);
        int nodeIdentifier = ByteBuffer.wrap(buffer).getInt();
        switch (nodeIdentifier) {
            case DirectoryNode.DIRECTORY_NODE_IDENTIFIER:
                return DirectoryNode.deserialize(ByteBuffer.wrap(buffer), parent);

            case LeafNode.LEAF_NODE_IDENTIFIER:
                return LeafNode.deserialize(ByteBuffer.wrap(buffer), parent);

            default:
                throw new Exception("Invalid node type.");
        }
    }

    static Node read(ByteBuffer byteBuffer, DirectoryNode parent) throws Exception {
        // mark te start of the byte buffer so we can reset back to this.
        byteBuffer.mark();
        int nodeIdentifier = byteBuffer.getInt();
        switch (nodeIdentifier) {
            case DirectoryNode.DIRECTORY_NODE_IDENTIFIER:
                byteBuffer.reset();
                return DirectoryNode.deserialize(byteBuffer, parent);

            case LeafNode.LEAF_NODE_IDENTIFIER:
                byteBuffer.reset();
                return LeafNode.deserialize(byteBuffer, parent);

            default:
                throw new Exception("Invalid node type.");
        }
    }

    abstract String search(String key) throws Exception;

    abstract String getStartKey() throws Exception;
}
