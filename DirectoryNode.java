import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.HashMap;
import org.apache.log4j.Logger;


// The SBTree Directory node structure is as follows.
// |Number of Sub Nodes | MPB Header | Page Number | Seperator Length | Seperator | Page Number | ... | MPB Header|...|
@Getter
@Setter
public class DirectoryNode extends Node {
    static Logger logger = Logger.getLogger(DirectoryNode.class);
    List<MultiPageBlockHeader> multiPageBlockHeaders = new ArrayList<>();
    List<Integer> subNodes = new ArrayList<>();
    List<String> seperatorKeys = new ArrayList<>();
    Integer mergeSubNodeCursor = 0;
    Integer mergeMultiPageBlockHeaderCursor = 0;
    static final int DIRECTORY_NODE_IDENTIFIER = 1;
    static final int MAX_KEYS = 10;
    DirectoryNode parent;
    static DirectoryNode root;
    static Map<DirectoryNode, Integer> offsetMap = new HashMap<>();
    static final String ROOT_FILE_NAME = "root.lsm";

    DirectoryNode(DirectoryNode parent) {
        this.parent = parent;
    }

    void adjustMergeMultiPageBlockHeaderCursor() {
        // we use the max + 1 cursor to decide when to move onto next sibling.
        // but why are we invoking this in that condition, find it.
        if (mergeSubNodeCursor == subNodes.size()) {
            return;
        }

        MultiPageBlockHeader multiPageBlockHeader = getMultiPageBlockHeader(mergeSubNodeCursor);

        // there is only one instance of each multi page block header.
        mergeMultiPageBlockHeaderCursor = multiPageBlockHeaders.indexOf(multiPageBlockHeader);
    }

    void incrementMergeCursor() {
        mergeSubNodeCursor++;
        // re-evaluate location of mergeMultiPageBlockHeaderCursor.
        adjustMergeMultiPageBlockHeaderCursor();
    }

    void adjustMergeCursor(int adjustment) {
        mergeSubNodeCursor += adjustment;
        // re-evaluate location of mergeMultiPageBlockHeaderCursor.
        adjustMergeMultiPageBlockHeaderCursor();
    }

    void resetMergeCursor() {
        mergeSubNodeCursor = 0;
        mergeMultiPageBlockHeaderCursor = 0;
    }

    void removeSubNodeFromMultiPageBlock() throws Exception {
        MultiPageBlockHeader multiPageBlockHeader = getMultiPageBlockHeaderAtCursor(0);
        // lets revisit if we need to uncomment this.
//        multiPageBlockHeader.updateCount(this, false);
        multiPageBlockHeader.decrementCountForNode();
        if (multiPageBlockHeader.isEmptyForNode()) {
            multiPageBlockHeaders.remove(multiPageBlockHeader);
            if (multiPageBlockHeader.inNext) {
                removeInPreviousForNextNode(multiPageBlockHeader);
            }
            if (multiPageBlockHeader.inPrevious) {
                removeInNextForPreviousNode(multiPageBlockHeader);
            }
        }
    }

    void removeInNextForPreviousNode(MultiPageBlockHeader multiPageBlockHeader) throws Exception {
        DirectoryNode.resetOffsetMap();

        DirectoryNode previousNode = getPreviousNode();
        Iterator<MultiPageBlockHeader> multiPageBlockHeaderIterator = previousNode.getMultiPageBlockHeaders().iterator();
        for (; multiPageBlockHeaderIterator.hasNext();) {
            MultiPageBlockHeader candidateMultiPageBlockHeader = multiPageBlockHeaderIterator.next();
            if (candidateMultiPageBlockHeader.getMultiPageBlockNumber() == multiPageBlockHeader.getMultiPageBlockNumber()) {
                candidateMultiPageBlockHeader.setInNext(false);
            }
        }
        previousNode.persistNodeInformation();
    }

    void removeInPreviousForNextNode(MultiPageBlockHeader multiPageBlockHeader) throws Exception {
        DirectoryNode.resetOffsetMap();

        DirectoryNode nextNode = getNextNode();
        Iterator<MultiPageBlockHeader> multiPageBlockHeaderIterator = nextNode.getMultiPageBlockHeaders().iterator();
        for (; multiPageBlockHeaderIterator.hasNext();) {
            MultiPageBlockHeader candidateMultiPageBlockHeader = multiPageBlockHeaderIterator.next();
            if (candidateMultiPageBlockHeader.getMultiPageBlockNumber() == multiPageBlockHeader.getMultiPageBlockNumber()) {
                candidateMultiPageBlockHeader.setInNext(false);
            }
        }
        nextNode.persistNodeInformation();
    }

    void removeSubNodeAtCursor() throws Exception {
        // We only remove the previous seperator key if it exists as that corresponds to the first key of the node being removed.
        // this ensures the invariant when the subnodes added only add seperators corresponding to their first key.
        // The reason for this is the fact that sibling nodes may not be present in memory so querying them is inefficient.
        subNodes.remove(mergeSubNodeCursor.intValue());
        if (mergeSubNodeCursor != 0) {
            seperatorKeys.remove(mergeSubNodeCursor.intValue() - 1);
        }

        removeSubNodeFromMultiPageBlock();
    }

    boolean cursorAtEnd() {
        return mergeSubNodeCursor >= subNodes.size();
    }

    boolean isEmpty() {
        return subNodes.isEmpty();
    }

    boolean shouldSplit() {
        return subNodes.size() >= MAX_KEYS;
    }

    static DirectoryNode getRoot() {
        if (root == null) {
            root = readRoot();
        }

        return root;
    }

    static void setRoot(DirectoryNode node) {
        root = node;
    }

    static void makeNewRoot(DirectoryNode child1, DirectoryNode child2, String seperatorKey, int childAtCursor) throws Exception {
        // Create single page node for new root.
        DirectoryNode newRoot = new DirectoryNode(null);
        MultiPageBlockHeader multiPageBlockHeader = MultiPageBlock.allocate();
        MultiPageBlock multiPageBlock = MultiPageBlock.get(multiPageBlockHeader);

        // Add both children into first 2 pages of multi page block.
        // and add the 2 children and the seperator to the new root.
        newRoot.subNodes.add(0);
        multiPageBlockHeader.incrementCount();
        multiPageBlockHeader.incrementCountForNode();
        multiPageBlock.incrementActivePages();
        child1.setParent(newRoot);
        child1.writeToMultiPageBlock(multiPageBlock, 0);

        newRoot.subNodes.add(1);
        multiPageBlockHeader.incrementCount();
        multiPageBlockHeader.incrementCountForNode();
        multiPageBlock.incrementActivePages();
        child2.setParent(newRoot);
        child2.writeToMultiPageBlock(multiPageBlock, 1);

        newRoot.seperatorKeys.add(seperatorKey);
        newRoot.multiPageBlockHeaders.add(multiPageBlockHeader);
        newRoot.mergeSubNodeCursor = childAtCursor;

        setRoot(newRoot);
        writeRoot();
    }

    int splitMultiPageBlockHeader(MultiPageBlockHeader nodeMultiPageBlockHeader, DirectoryNode sibling, boolean insertAfter) throws Exception {
        logger.info("Multipage blocks are splitting");
        int siblingPageNumber;
        MultiPageBlockHeader siblingMultiPageBlockHeader = nodeMultiPageBlockHeader.split(this, insertAfter);
        MultiPageBlock nodeMultiPageBlock = MultiPageBlock.get(nodeMultiPageBlockHeader);
        MultiPageBlock siblingMultiPageBlock = MultiPageBlock.get(siblingMultiPageBlockHeader);

        if (siblingMultiPageBlockHeader == nodeMultiPageBlockHeader) {
            // insert sibling data into next available page of Multi Block indicated by nodeMultiPageBlockHeader
            siblingPageNumber = nodeMultiPageBlockHeader.getCount();
            nodeMultiPageBlockHeader.updateCount(this, true);
            nodeMultiPageBlockHeader.incrementCountForNode();
            sibling.writeToMultiPageBlock(nodeMultiPageBlock, siblingPageNumber);
            // do we need to update multi page block as well
            nodeMultiPageBlock.incrementActivePages();
        } else {
            // insert sibling data into next available page of Multi Block indicated by siblingMultiPageBlockHeader
            siblingPageNumber = siblingMultiPageBlockHeader.getCount();

            // update in the persistent store.
            siblingMultiPageBlockHeader.updateCount(this, true);
            siblingMultiPageBlockHeader.incrementCountForNode();
            sibling.writeToMultiPageBlock(siblingMultiPageBlock, siblingPageNumber);
            siblingMultiPageBlock.incrementActivePages();
        }

        return siblingPageNumber;
    }

    void insertSiblingIntoParent(DirectoryNode parent, DirectoryNode sibling, String medianSeperatorKey, boolean insertAfter) throws Exception {
        MultiPageBlockHeader nodeMultiPageBlockHeader = parent.getMultiPageBlockHeaderAtCursor(0);

        int siblingPageNumber;
        if (nodeMultiPageBlockHeader.isFull()) {
            siblingPageNumber = parent.splitMultiPageBlockHeader(nodeMultiPageBlockHeader, sibling, insertAfter);
        } else {
            MultiPageBlock nodeMultiPageBlock = MultiPageBlock.get(nodeMultiPageBlockHeader);
            // insert sibling data into next available page of Multi Block indicated by nodeMultiPageBlockHeader
            siblingPageNumber = nodeMultiPageBlockHeader.getCount();
            nodeMultiPageBlockHeader.updateCount(parent, true);
            nodeMultiPageBlockHeader.incrementCountForNode();
            nodeMultiPageBlock.incrementActivePages();
            sibling.writeToMultiPageBlock(nodeMultiPageBlock, siblingPageNumber);
        }
        if (insertAfter) {
            parent.insertSubNodeAfterCursor(siblingPageNumber, medianSeperatorKey, 1);
        } else {
            parent.insertSubNodeAtCursor(siblingPageNumber, medianSeperatorKey, 1);
        }

        if (parent != getRoot()) {
            parent.writeToMultiPageBlockAtCursor();
        } else {
            writeRoot();
        }
    }

    void setupNodeInformation(List<Integer> subNodes, List<String> seperatorKeys, List<MultiPageBlockHeader> multiPageBlockHeaders) {
        setSubNodes(subNodes);
        setSeperatorKeys(seperatorKeys);
        setMultiPageBlockHeaders(multiPageBlockHeaders);
    }

    // this node split has to optimize and adjust for the current merge cursor.
    // in order to keep the callers semantics invariant it ensures the merge cursor remains in the splitting node by
    // having the sibling positioned right or left accordingly.
    DirectoryNode split() throws Exception {
        // split the subnodes into 2.
        // split the seperator into 2 and move median to top.
        int median = subNodes.size()/2;
        List<Integer> leftSubNodes = new ArrayList<>(subNodes.subList(0, median));
        List<Integer> rightSubNodes = new ArrayList<>(subNodes.subList(median, subNodes.size()));
        List<String> leftSeperatorKeys = new ArrayList<>(seperatorKeys.subList(0, median - 1));
        String medianSeperatorKey = seperatorKeys.get(median - 1);
        List<String> rightSeperatorKeys = new ArrayList<>(seperatorKeys.subList(median, seperatorKeys.size()));
        MultiPageBlockHeader leftMultiBlockPageHeader = getMultiPageBlockHeader(median - 1);
        MultiPageBlockHeader rightMultiBlockPageHeader = getMultiPageBlockHeader(median);
        List<MultiPageBlockHeader> leftMultiPageBlockHeaders;
        List<MultiPageBlockHeader> rightMultiPageBlockHeaders;

        if (leftMultiBlockPageHeader.multiPageBlockNumber == rightMultiBlockPageHeader.multiPageBlockNumber) {
            // we need to include the header in both left and right subtrees.
            int index = multiPageBlockHeaders.indexOf(leftMultiBlockPageHeader);
            leftMultiPageBlockHeaders = new ArrayList<>(multiPageBlockHeaders.subList(0, index + 1));
            rightMultiPageBlockHeaders = new ArrayList<>(multiPageBlockHeaders.subList(index, multiPageBlockHeaders.size()));

            // sum all the block headers until the index one. create copies of the splitting block header.
            // setup the inNext inPrevious flags. adjust the count for nodes in the 2 headers.
            MultiPageBlockHeader leftSplittingMultiPageBlockHeader =  multiPageBlockHeaders.get(index).copy();
            MultiPageBlockHeader rightSplittingMultiPageBlockHeader =  multiPageBlockHeaders.get(index).copy();
            // once we persist directory nodes lets undo this.
            leftSplittingMultiPageBlockHeader.inNext = true;
            rightSplittingMultiPageBlockHeader.inPrevious = true;
            // count the subNodes until the split.
            int splitSubNodeCount = -median;
            for (MultiPageBlockHeader multiPageBlockHeader: leftMultiPageBlockHeaders) {
                splitSubNodeCount += multiPageBlockHeader.getCountForNode();
            }

            leftSplittingMultiPageBlockHeader.adjustCountForNode(splitSubNodeCount);
            rightSplittingMultiPageBlockHeader.setCountForNode(splitSubNodeCount);
            leftMultiPageBlockHeaders.remove(index);
            rightMultiPageBlockHeaders.remove(0);
            leftMultiPageBlockHeaders.add(leftSplittingMultiPageBlockHeader);
            rightMultiPageBlockHeaders.add(0, rightSplittingMultiPageBlockHeader);
        } else {
            int index = multiPageBlockHeaders.indexOf(rightMultiBlockPageHeader);
            leftMultiPageBlockHeaders = new ArrayList<>(multiPageBlockHeaders.subList(0, index));
            rightMultiPageBlockHeaders = new ArrayList<>(multiPageBlockHeaders.subList(index, multiPageBlockHeaders.size()));
        }

        // the filling block needs to be added to the list that is undergoing the merge.
        DirectoryNode sibling = new DirectoryNode(parent);
        // is the splitted node to the left or to the right.
        // lets ensure the cursor is always in the caller's object to ensure caller's invariants remain.
        if (mergeSubNodeCursor < median) {
            setupNodeInformation(leftSubNodes, leftSeperatorKeys, leftMultiPageBlockHeaders);
            sibling.setupNodeInformation(rightSubNodes, rightSeperatorKeys, rightMultiPageBlockHeaders);
            // insert the sibling correctly into the parent directory node.
            // right sibling needs to be in the same multi page block as the node if there is a following node in same mpb.
            if (parent != null) {
                insertSiblingIntoParent(parent, sibling, medianSeperatorKey, true);
            } else {
                makeNewRoot(this, sibling, medianSeperatorKey, 0);
            }
        } else {
            setupNodeInformation(rightSubNodes, rightSeperatorKeys, rightMultiPageBlockHeaders);
            sibling.setupNodeInformation(leftSubNodes, leftSeperatorKeys, leftMultiPageBlockHeaders);
            // update the merge subnode cursor.
            adjustMergeCursor(-sibling.getSubNodes().size());
            if (parent != null) {
                insertSiblingIntoParent(parent, sibling, medianSeperatorKey, false);
            } else {
                makeNewRoot(sibling, this, medianSeperatorKey, 1);
            }
        }

        if (parent != null && parent.shouldSplit()) {
            parent.split();
            parent.writeToMultiPageBlockAtCursor();
        }

        return sibling;
    }

    static void resetOffsetMap() {
        offsetMap.clear();
    }

    // this is not a general routine but for specific purposes only.
    // assumption is that the previous node has the same MP block.
    DirectoryNode getPreviousNode() throws Exception {
        DirectoryNode candidateParent = parent;
        if (!offsetMap.containsKey(candidateParent)) {
            offsetMap.put(candidateParent, candidateParent.mergeSubNodeCursor);
        }

        int previousOffset = offsetMap.get(candidateParent) - 1;
        if (previousOffset < 0) {
            if (candidateParent != getRoot()) {
                candidateParent = candidateParent.getPreviousNode();
            }
            previousOffset = candidateParent.subNodes.size() - 1;
        }

        offsetMap.put(candidateParent, previousOffset);

        MultiPageBlockHeader multiPageBlockHeader = candidateParent.getMultiPageBlockHeader(previousOffset);
        MultiPageBlock multiPageBlock = MultiPageBlock.get(multiPageBlockHeader);
        int previousNodePageOffset = candidateParent.subNodes.get(previousOffset);

        return (DirectoryNode) Node.read(multiPageBlock.getPageBuffer(previousNodePageOffset), candidateParent);
    }

    // this is not a general routine but for specific purposes only.
    // assumption is that the previous node has the same MP block.
    // offset map contains the current node being processed by the iterators getPreviousNode and getNextNode.
    DirectoryNode getNextNode() throws Exception {
        DirectoryNode candidateParent = parent;
        if (!offsetMap.containsKey(candidateParent)) {
            offsetMap.put(candidateParent, candidateParent.mergeSubNodeCursor);
        }

        int nextOffset = offsetMap.get(candidateParent) + 1;
        if (nextOffset >= candidateParent.subNodes.size()) {
            if (candidateParent != getRoot()) {
                candidateParent = candidateParent.getNextNode();
            }
            nextOffset = 0;
        }

        offsetMap.put(candidateParent, nextOffset);

        MultiPageBlockHeader multiPageBlockHeader = candidateParent.getMultiPageBlockHeader(nextOffset);
        MultiPageBlock multiPageBlock = MultiPageBlock.get(multiPageBlockHeader);
        Integer nextNodePageOffset = candidateParent.subNodes.get(nextOffset);

        return (DirectoryNode) Node.read(multiPageBlock.getPageBuffer(nextNodePageOffset), candidateParent);
    }

    void insertSubNodeAtCursor(int node, String seperatorKey, int separatorOffset) {
        int seperatorIndex = mergeSubNodeCursor - 1 + separatorOffset;

        subNodes.add(mergeSubNodeCursor, node);
        // We only add the provided seperator as needed.
        if (seperatorIndex >= 0) {
            seperatorKeys.add(seperatorIndex, seperatorKey);
        }
        mergeSubNodeCursor++;
    }

    void insertSubNodeAfterCursor(int node, String seperatorKey, int offset) {
        subNodes.add(mergeSubNodeCursor + offset, node);
        seperatorKeys.add(mergeSubNodeCursor - 1 + offset, seperatorKey);
    }

    void insertMultiPageBlockHeaderAtCursor(MultiPageBlockHeader multiPageBlockHeader) {
        multiPageBlockHeaders.add(mergeMultiPageBlockHeaderCursor, multiPageBlockHeader);
        mergeMultiPageBlockHeaderCursor++;
    }

    MultiPageBlockHeader getMultiPageBlockHeaderAtCursor(int offset) {
        return multiPageBlockHeaders.get(mergeMultiPageBlockHeaderCursor + offset);
    }

    void serialize(ByteBuffer bb) {
        int subNodeIndex = 0;
        bb.putInt(DIRECTORY_NODE_IDENTIFIER);
        bb.putInt(subNodes.size());

        // its not clear whether SBTree uses a seperator between 2 MPBs, but this is required for quick indexed finds.
        // it is best to save the seperator in the directory node, so you do not need to read the MPB.
        for (MultiPageBlockHeader multiPageBlockHeader: multiPageBlockHeaders) {
            multiPageBlockHeader.serialize(bb);
            for (int i = 0; i < multiPageBlockHeader.getCountForNode(); i++, subNodeIndex++)  {
                bb.putInt(subNodes.get(subNodeIndex));
                if (subNodeIndex < subNodes.size() - 1) {
                    Utils.serializeString(bb, seperatorKeys.get(subNodeIndex));
                }
            }
        }
    }

    static DirectoryNode deserialize(ByteBuffer bb, DirectoryNode parent) throws Exception {
        DirectoryNode directoryNode = new DirectoryNode(parent);

        if (bb.getInt() != DIRECTORY_NODE_IDENTIFIER) {
            throw new Exception("Expecting Directory but found wrong node type.");
        }

        int numSubNodes = bb.getInt();
        for (int nodesAdded = 0; nodesAdded < numSubNodes;) {
            MultiPageBlockHeader multiPageBlockHeader = MultiPageBlockHeader.deserialize(bb);
            directoryNode.multiPageBlockHeaders.add(multiPageBlockHeader);
            for (int j = 0; j < multiPageBlockHeader.getCountForNode(); j++, nodesAdded++) {
                directoryNode.subNodes.add(bb.getInt());
                if (nodesAdded < numSubNodes - 1) {
                    String seperatorKey = Utils.deserializeString(bb);
                    directoryNode.seperatorKeys.add(seperatorKey);
                }
            }
        }

        return directoryNode;
    }

    MultiPageBlockHeader getMultiPageBlockHeader(int pageNumberOffset) {
        int i;
        // Which multi page block does the ith page exist in?
        for (i = 0; i < multiPageBlockHeaders.size(); i++) {
            int pageCount = multiPageBlockHeaders.get(i).getCountForNode();

            if (pageNumberOffset == 0 || pageNumberOffset < pageCount) {
                break;
            } else {
                pageNumberOffset -= pageCount;
            }
        }

        if (i < multiPageBlockHeaders.size()) {
            return multiPageBlockHeaders.get(i);
        } else {
            return null;
        }
    }

    String search(String key) {
        int i;
        // the key cannot be found here, all you can do is direct this to the correct leaf or directory.
        for (i = 0; i < seperatorKeys.size(); i++) {
            if (seperatorKeys.get(i).compareTo(key) < 0) {
                break;
            }
        }

        try {
            // load subnode[i] into memory and search there.
            return Node.read(getMultiPageBlockHeader(i), subNodes.get(i), this).search(key);
        } catch (Exception e) {
            return null;
        }
    }

    Node getChildAtCursor() throws Exception {
        if (!cursorAtEnd()) {
            Integer pageNumber = subNodes.get(mergeSubNodeCursor);
            // should the queries be for the mergeMultiPageBlockHeaderCursor
            MultiPageBlockHeader multiPageBlockHeader = getMultiPageBlockHeader(mergeSubNodeCursor);
            MultiPageBlock multiPageBlock = MultiPageBlock.get(multiPageBlockHeader);
            ByteBuffer byteBuffer = multiPageBlock.getPageBuffer(pageNumber);
            Node child = Node.read(byteBuffer, this);

            return child;
        }

        return null;
    }

    static DirectoryNode readRoot() {
        try {
            FileInputStream file = new FileInputStream(new File(ROOT_FILE_NAME));

            byte[] buffer = new byte[Node.PAGE_SIZE];
            file.read(buffer);

            return DirectoryNode.deserialize(ByteBuffer.wrap(buffer), null);
        } catch (Exception e) {
            logger.info("Exception reading root...");
        }

        return null;
    }

    static void writeRoot() {
        try {
            FileOutputStream file = new FileOutputStream(new File(ROOT_FILE_NAME));
            byte[] buffer = new byte[Node.PAGE_SIZE];
            root.serialize(ByteBuffer.wrap(buffer));

            file.write(buffer);
        } catch (Exception e) {
            logger.info("Exception writing root...");
        }
    }

    // starting with the subnode
    void replaceSubNodes(int subNodeOffset, int multiPageOffset, int count) {
        int j = 0;
        for (int i = subNodeOffset; i < subNodeOffset + count; i++, j++) {
            subNodes.set(i, j + multiPageOffset);
        }
    }

    String getStartKey() {
        try {
            // load subnode[i] into memory and search there.
            return Node.read(getMultiPageBlockHeader(0), subNodes.get(0), this).getStartKey();
        } catch (Exception e) {
            return null;
        }
    }

    void writeToMultiPageBlock(MultiPageBlock multiPageBlock, int pageOffset) throws Exception {
        ByteBuffer bb = multiPageBlock.getPageBuffer(pageOffset);
        serialize(bb);
    }

    boolean isMultiPageBlockHeaderPresent(int multiPageBlockNumber) {
        for (MultiPageBlockHeader multiPageBlockHeader : multiPageBlockHeaders) {
            if (multiPageBlockHeader.getMultiPageBlockNumber() == multiPageBlockNumber) {
                return true;
            }
        }

        return false;
    }

    MultiPageBlockHeader getMatchingMultiPageBlockHeader(int multiPageBlockNumber) {
        for (MultiPageBlockHeader multiPageBlockHeader : multiPageBlockHeaders) {
            if (multiPageBlockHeader.getMultiPageBlockNumber() == multiPageBlockNumber) {
                return multiPageBlockHeader;
            }
        }

        return null;
    }

    void writeToMultiPageBlock(int cursorOffset) throws Exception {
        DirectoryNode parent = getParent();
        MultiPageBlockHeader multiPageBlockHeader = parent.getMultiPageBlockHeader(parent.mergeSubNodeCursor + cursorOffset);
        int offset = parent.getSubNodes().get(parent.mergeSubNodeCursor + cursorOffset);

        writeToMultiPageBlock(MultiPageBlock.get(multiPageBlockHeader), offset);
    }

    MultiPageBlockHeader copyAndSetupMultiPageBlockHeader(MultiPageBlockHeader multiPageBlockHeader, int countForNode) {
        MultiPageBlockHeader copyMultiPageBlockHeader = multiPageBlockHeader.copy();
        // ideally the sibling's multipage block header would be linked, but since merge progresses
        // ahead this is unnecessary.
        copyMultiPageBlockHeader.setCountForNode(countForNode);
        insertMultiPageBlockHeaderAtCursor(copyMultiPageBlockHeader);

        return copyMultiPageBlockHeader;
    }

    void writeToMultiPageBlockAtCursor() throws Exception {
        DirectoryNode parent = getParent();
        MultiPageBlockHeader parentMultiPageBlockHeader = parent.getMultiPageBlockHeaderAtCursor(0);
        MultiPageBlock parentMultiPageBlock = MultiPageBlock.get(parentMultiPageBlockHeader);
        Integer parentPageNumber = parent.subNodes.get(parent.mergeSubNodeCursor);

        writeToMultiPageBlock(parentMultiPageBlock, parentPageNumber);
    }

    void persistNodeInformation() throws Exception {
        DirectoryNode parent = getParent();
        int offset = DirectoryNode.offsetMap.get(parent);
        MultiPageBlockHeader multiPageBlockHeader = parent.getMultiPageBlockHeader(offset);
        int nodePageOffset = parent.getSubNodes().get(offset);
        writeToMultiPageBlock(MultiPageBlock.get(multiPageBlockHeader), nodePageOffset);
    }
}
