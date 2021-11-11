import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.log4j.Logger;

@Getter @Setter
public class LevelMerge extends Thread {
    int merge;
    static Logger logger = Logger.getLogger(LevelMerge.class);
    MemoryComponent memoryComponent = MemoryComponent.getInstance();
    String nextKey;
    List<MultiPageBlock> fillingBlocks = new ArrayList<>();
    LeafNode currentFillingLeaf;
    LeafNode previousEmptyingLeaf;
    MultiPageBlockHeader currentFillingMultiPageBlockHeader;
    DirectoryNode currentMergingNode;
    List<Integer> mergePath = new ArrayList<>();

    LeafNode getNextFillingLeaf(DirectoryNode parent) throws Exception {
        // persist the current filling leaf.
        writeCurrentFillingLeaf();

        if (fillingBlocks.isEmpty() || getFillingBlock().isFull()) {
            MultiPageBlockHeader multiPageBlockHeader = MultiPageBlock.allocate();
            currentFillingMultiPageBlockHeader = multiPageBlockHeader;
            logger.warn("New filling block created with block number - " + multiPageBlockHeader.getMultiPageBlockNumber());
            fillingBlocks.add(MultiPageBlock.get(multiPageBlockHeader));
            // insert Filling MultiPageblockHeader at the parent's current merge cursor and increment the cursor.
            parent.insertMultiPageBlockHeaderAtCursor(multiPageBlockHeader);
        }

        return new LeafNode(parent);
    }

    MultiPageBlock getFillingBlock() {
        return fillingBlocks.get(fillingBlocks.size() - 1);
    }

    boolean isLastEmptyingLeaf(LeafNode emptyingLeaf) {
        DirectoryNode emptyingParent = emptyingLeaf.getParent();
        while (emptyingParent != null && emptyingParent.cursorAtEnd()) {
            emptyingParent = emptyingParent.getParent();
        }

        // cursor was at the end for all parents including root, implying this is the last leaf.
        return emptyingParent == null;
    }

    // We do an initial merge to populate the root asap.
    void doInitialMerge() throws Exception {
        DirectoryNode root = DirectoryNode.getRoot();
        List<KeyData> mergingList = memoryComponent.removeKeyDataForMerge(null, null);

        Iterator<KeyData> mergingIterator = mergingList.iterator();
        KeyData mergingEntry = mergingIterator.next();

        while (mergingEntry != null) {
            KeyData fillingEntry = mergingEntry;
            if (mergingIterator.hasNext()) {
                mergingEntry = mergingIterator.next();
            } else {
                mergingEntry = null;
            }
            if (currentFillingLeaf == null || currentFillingLeaf.isFull()) {
                // find the next filling leaf and add it to the root.
                // not much risk of root spilling here. as we can control initial merge to be less than a multiblock.
                currentFillingLeaf = getNextFillingLeaf(root);

                // the previous routine could cause the filling block to change.
                MultiPageBlock fillingBlock = getFillingBlock();
                root.insertSubNodeAtCursor(fillingBlock.getActivePages(), fillingEntry.getKey(), 0);
                // update the active pages and count corresponding to the multi page block.
                fillingBlock.incrementActivePages();
                MultiPageBlockHeader rootMultiPageBlockHeader = root.getMultiPageBlockHeaderAtCursor(-1);
                // the root has no siblings sharing the header so this is a single node increment.
                rootMultiPageBlockHeader.incrementCount();
                rootMultiPageBlockHeader.incrementCountForNode();
            }
            currentFillingLeaf.add(fillingEntry);
        }
        writeCurrentFillingLeaf();
        reinitializeMergeState();
        // how do we write the leaf nodes back to multi page block, what about dir nodes.
        // when do dirty multi page blocks get written back to disk.
        // we possibly need to identify how to recover as well.
        DirectoryNode.writeRoot();
    }

    void reinitializeMergeState() {
        DirectoryNode root = DirectoryNode.getRoot();

        // clear the filling blocks and reset the current filling leaf.
        previousEmptyingLeaf = null;
        currentFillingLeaf = null;
        fillingBlocks.clear();

        // reset the merge cursor for the root.
        root.resetMergeCursor();
    }

    List<KeyData> getKeyForRange(LeafNode startLeaf, LeafNode leaf) {
        // we need to identify the previous leaf so the range captured includes keys after the endKey of previous leaf.
        // we also need to know if this is the last leaf so as to encompass the rest of the range.
        // remove the keys in the overlapping key range from earlier component.
        String rangeStartKey, rangeEndKey;
        if (startLeaf != null) {
            rangeStartKey = startLeaf.getEndKey();
        } else {
            rangeStartKey = null;
        }

        if (!isLastEmptyingLeaf(leaf)) {
            rangeEndKey = leaf.getEndKey();
        } else {
            rangeEndKey = null;
        }

        logger.warn(rangeStartKey);
        logger.warn(rangeEndKey);

        if (rangeStartKey != null && rangeEndKey != null && (rangeStartKey.equals("key296") || rangeEndKey.equals("key1014"))) {
            logger.info("");
        }

        return memoryComponent.removeKeyDataForMerge(rangeStartKey, rangeEndKey);
    }

    void writeCurrentFillingLeaf() throws Exception {
        if (currentFillingLeaf == null) {
            return;
        }
        MultiPageBlock fillingBlock = getFillingBlock();
        currentFillingLeaf.writeToMultiPageBlock(fillingBlock, fillingBlock.getActivePages() - 1);
    }

    // this will merge all the key data in a leaf with all the keys extracted from the earlier component.
    // while we are reading from emptying block we dont expect to mutate it, so we dont need to worry about keeping it uptodate
    // as we remove the emptying leaf and replace the directory nodes with filling leafs.
    void doMergeStep() throws Exception {
        // remove the emptying leaf node from the directory node, but leave the seperator if any exists, so it could be
        // used to seperate the filling leaf that will be subsequently added.
        LeafNode emptyingLeaf = removeNextLeafForMerge();
        DirectoryNode parent = emptyingLeaf.getParent();
        logger.info("starting merge step:: emptying leaf, parent");
        logger.info(emptyingLeaf);
        logger.info(parent);

        List<KeyData> mergingList = getKeyForRange(previousEmptyingLeaf, emptyingLeaf);
        // iterate through the data to do the merge.
        Iterator<KeyData> emptyingIterator = emptyingLeaf.getKeyDataList().iterator();
        Iterator<KeyData> mergingIterator = mergingList.iterator();
        KeyData emptyingEntry = emptyingIterator.next();
        // There maybe no keys in the range for the emptying leaf.
        KeyData mergingEntry = mergingIterator.hasNext() ? mergingIterator.next() : null;

        while (mergingEntry != null || emptyingEntry != null) {
            KeyData fillingEntry;
            int keyCompare = emptyingEntry == null ? 1 : mergingEntry == null ? -1 : emptyingEntry.getKey().compareTo(mergingEntry.getKey());
            if (keyCompare > 0) {
                fillingEntry = mergingEntry;
                mergingEntry = mergingIterator.hasNext() ? mergingIterator.next() : null;
            } else if (keyCompare < 0) {
                fillingEntry = emptyingEntry;
                emptyingEntry = emptyingIterator.hasNext() ? emptyingIterator.next() : null;
            } else {
                // merge the 2 entries and move that to new leaf.
                fillingEntry = KeyData.merge(emptyingEntry, mergingEntry);
                mergingEntry = mergingIterator.hasNext() ? mergingIterator.next() : null;
                emptyingEntry = emptyingIterator.hasNext() ? emptyingIterator.next() : null;
            }


            if (currentFillingLeaf == null || currentFillingLeaf.isFull()) {
                currentFillingLeaf = getNextFillingLeaf(parent);
                // the filling leaf will precede any existing emptying leafs.
                // Insert the next active page index into the directory with the start key as the seperator from previous node.
                // If the filling page is the first one, then there is no need to add a seperator.
                MultiPageBlock fillingBlock = getFillingBlock();
                parent.insertSubNodeAtCursor(fillingBlock.getActivePages(), fillingEntry.getKey(), 0);
                // update the active pages and count corresponding to the multi page block.
                fillingBlock.incrementActivePages();

                MultiPageBlockHeader fillingMultiPageBlockHeader = parent.getMatchingMultiPageBlockHeader(currentFillingMultiPageBlockHeader.getMultiPageBlockNumber());
                if (fillingMultiPageBlockHeader == null) {
                    fillingMultiPageBlockHeader = parent.copyAndSetupMultiPageBlockHeader(currentFillingMultiPageBlockHeader, 0);
                }

                fillingMultiPageBlockHeader.updateCount(parent, true);
                fillingMultiPageBlockHeader.incrementCountForNode();
                currentFillingMultiPageBlockHeader = fillingMultiPageBlockHeader;
                // as soon as the parent becomes consistent lets write it so that further routines have the correct information.
                if (parent != DirectoryNode.getRoot()) {
                    parent.writeToMultiPageBlockAtCursor();
                } else {
                    DirectoryNode.writeRoot();
                }

                // check if the parent should be split after the insert and do so.
                if (parent.shouldSplit()) {
                    // even after the split the merge will proceed in what is the current parent.
                    // depending on merge cursor the parent will either be the left half or right half, and merge cursor
                    // needs to be setup correctly for both nodes. also need to add the new node and seperator to parent's parent.
                    logger.info("Node before splitting");
                    logger.info(parent);
                    logger.info("Node before splitting done");

                    DirectoryNode sibling = parent.split();
                    logger.info("Nodes after splitting");
                    logger.info(parent);
                    logger.info(sibling);
                    logger.info("Nodes after splitting done");

                    if (!parent.isMultiPageBlockHeaderPresent(fillingMultiPageBlockHeader.getMultiPageBlockNumber())) {
                        MultiPageBlockHeader copyFillingMultiPageBlockHeader = parent.copyAndSetupMultiPageBlockHeader(fillingMultiPageBlockHeader, 1);
                        currentFillingMultiPageBlockHeader = copyFillingMultiPageBlockHeader;

                        // we are splitting right on the filling page just added, so move it to this node.
                        int lastSiblingIndex = sibling.getSubNodes().size() - 1;
                        MultiPageBlockHeader siblingMultiPageBlockHeader = sibling.getMultiPageBlockHeader(lastSiblingIndex);
                        Integer fillingPage = sibling.getSubNodes().remove(lastSiblingIndex);
                        siblingMultiPageBlockHeader.decrementCountForNode();
                        String newParentSeperatorKey = sibling.getSeperatorKeys().remove(sibling.getSeperatorKeys().size() - 1);
                        String currentParentSeperatorKey = parent.getParent().getSeperatorKeys().set(parent.getParent().mergeSubNodeCursor - 1, newParentSeperatorKey);
                        // Add the subnode and median seperator key to the emptying leafs parent node.
                        if (parent.getMergeSubNodeCursor() != 0) {
                            logger.info("This is screwed up.");
                        }
                        parent.insertSubNodeAtCursor(fillingPage, currentParentSeperatorKey, 1);
                        sibling.writeToMultiPageBlock(-1);

                        logger.info("Adjustments after the split.");
                        logger.info(parent);
                        logger.info(sibling);
                        logger.info(parent.getParent());
                    }
                }
            }
            currentFillingLeaf.add(fillingEntry);
        }

        // persist the leaf at the end of the merge step.
        // update the parent of the leaf onto its multiblock.
        writeCurrentFillingLeaf();
        if (parent.getParent() == null) {
            DirectoryNode.writeRoot();
        } else {
            parent.writeToMultiPageBlock(0);
        }

        // THe filling leaf should not span directory nodes, so set this to null.
        // Unlike Multi page blocks for directory nodes, we dont need to ensure that the duplicate headers are kept in sync.
        // as the progression of usage is in one direction only and there are no mutates.
        if (parent.cursorAtEnd()) {
            currentFillingLeaf = null;
        }

        // setup previous emptying leaf for next merge step, so as to query the appropriate range.
        // I think the previous end key should have sufficed.
        previousEmptyingLeaf = emptyingLeaf;
    }

    // TODO::
    // Traversing from root down to leaf on every occasion, if number of levels are low this is cheap.
    // but is this necessary for correctness or is it redundant?
    LeafNode removeNextLeafForMerge() throws Exception {
        DirectoryNode node;
        if (currentMergingNode != null) {
            node = currentMergingNode;
        } else {
            node = DirectoryNode.getRoot();
        }

        if (mergePath.size() >= 1) {
            mergePath.remove(mergePath.size() - 1);
        }
        // this is cursorified and hence resumable dfs.
        // we avoid tracking merge data for anything but the current path from root down to current merging node
        // this stateless behavior simplifies the algorithm.
        do {
            Node child = node.getChildAtCursor();

            if (child instanceof LeafNode) {
                // TODO:: we maybe able to track this more efficiently.
                // This however is simpler as it addresses correct setting for all possible scenarios.
                mergePath.add(node.getMergeSubNodeCursor());
                currentMergingNode = node;
                logger.info("This is the current merging node.");
                logger.info(currentMergingNode);
                node.removeSubNodeAtCursor();

                logMergePath(mergePath);

                if (previousEmptyingLeaf != null && previousEmptyingLeaf.keyDataList.get(previousEmptyingLeaf.keyDataList.size()-1).getKey().compareTo(((LeafNode) child).keyDataList.get(((LeafNode) child).keyDataList.size() - 1).getKey()) > 0) {
                    logger.info("anamolous condition caught before execution.");
                }
                return (LeafNode)child;
            } else if (child == null) {
                // dont reset the merge cursor for the node for the next time we query it.
                // if this is not the root, restart search from the parent with incremented merge cursor.
                if (node.parent != null) {
                    currentFillingLeaf = null;
                    node = node.parent;
                    node.incrementMergeCursor();
                    mergePath.add(node.getMergeSubNodeCursor());
                } else {
                    // for root we start back from initial point and reinitialize the merge state.
                    mergePath.clear();
                    reinitializeMergeState();
                }
            } else {
                node = (DirectoryNode) child;
            }
        } while (true);
    }

    void logMergePath(List<Integer> mergePath){
        logger.warn("Next step in merge is as follows");
        StringBuilder stringBuilder = new StringBuilder();
        for (Integer element : mergePath) {
            stringBuilder.append(element);
            stringBuilder.append(":");
        }
        logger.warn(stringBuilder.toString());
    }

    public void run() {
        while (true) {
            try {
                // check if threshold is exceeded.
                if (memoryComponent.exceedsThreshold()) {
                    if (DirectoryNode.getRoot().isEmpty()) {
                        logger.info("Doing initial merge.");
                        doInitialMerge();
                    } else {
                        logger.info("Running merge.");
                        doMergeStep();
                    }
                    logger.info("Root");
                    logger.info(DirectoryNode.getRoot());
                }
                logger.info("Sleeping for 10 seconds in merge.");
                Thread.sleep(1000);
            } catch (Exception e) {
                logger.info("Merge Step caused exception is this retryable??");
                e.printStackTrace();
                break;
            }
        }
    }
}

