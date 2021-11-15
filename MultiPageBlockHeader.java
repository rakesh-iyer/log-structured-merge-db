import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.Iterator;

@Getter @Setter
public class MultiPageBlockHeader extends Debuggable {
    static Logger logger = Logger.getLogger(MultiPageBlockHeader.class);
    int multiPageBlockNumber;
    int count; // number of active pages
    int size; // maximum number of pages
    int countForNode; // header can be shared amongst multiple directory nodes, so this is the count for this node.
    boolean inPrevious; // for shared headers this is used to find the end point directory nodes.
    boolean inNext; // for shared headers this is used to find the end point directory nodes.

    static Random blockNumberGenerator = new Random(System.currentTimeMillis());

    MultiPageBlockHeader(int multiPageBlockNumber, int count, int size, int countForNode, boolean inPrevious, boolean inNext) {
        this.multiPageBlockNumber = multiPageBlockNumber;
        this.count = count;
        this.size = size;
        this.countForNode = countForNode;
        this.inPrevious = inPrevious;
        this.inNext = inNext;
    }

    static MultiPageBlockHeader allocate(int count, int size, int countNode) {
        return new MultiPageBlockHeader(blockNumberGenerator.nextInt(), count, size, countNode, false, false);
    }

    void decrementCount() {
        count--;
    }

    void incrementCount() {
        count++;
    }

    void decrementCountForNode() {
        countForNode--;
    }

    void incrementCountForNode() {
        countForNode++;
    }

    void adjustCountForNode(int adjustment) {
        countForNode -= adjustment;
    }

    boolean isEmpty() {
        return count == 0;
    }

    boolean isEmptyForNode() {
        return countForNode == 0;
    }

    boolean isFull() { return count == size; }

    void serialize(ByteBuffer bb) {
        bb.putInt(multiPageBlockNumber);
        bb.putInt(count);
        bb.putInt(size);
        bb.putInt(countForNode);
        bb.put(Utils.getByte(inPrevious));
        bb.put(Utils.getByte(inNext));
    }

    static MultiPageBlockHeader deserialize(ByteBuffer bb) {
        return new MultiPageBlockHeader(bb.getInt(), bb.getInt(), bb.getInt(), bb.getInt(), Utils.getBoolean(bb.get()), Utils.getBoolean(bb.get()));
    }

    void updateCount(DirectoryNode node, boolean increment) throws Exception {
        DirectoryNode currentNode = node;
        int offsetFromSelectedNode = 0;
        logger.debug(String.format("update count for %s as increment:%s", node, increment));
        // Reset the offset map before doing this increment.
        // This means you cannot do 2 splits at the same time. This might not be a major bottleneck.
        DirectoryNode.resetOffsetMap();
        MultiPageBlockHeader candidateMultiPageBlockHeader = this;

        if (node == DirectoryNode.getRoot()) {
            if (increment) {
                incrementCount();
            } else {
                decrementCount();
            }
            return;
        }

        while (candidateMultiPageBlockHeader.inPrevious) {
            offsetFromSelectedNode++;
            currentNode = currentNode.getPreviousNode();

            Iterator<MultiPageBlockHeader> multiPageBlockHeaderIterator = currentNode.getMultiPageBlockHeaders().iterator();
            for (; multiPageBlockHeaderIterator.hasNext();) {
                candidateMultiPageBlockHeader = multiPageBlockHeaderIterator.next();
                if (candidateMultiPageBlockHeader.multiPageBlockNumber == multiPageBlockNumber) {
                    break;
                }
            }

            // in case the next pointer is somehow wrong lets terminate this based on block number.
            if (candidateMultiPageBlockHeader.multiPageBlockNumber != multiPageBlockNumber) {
                if (MultiPageBlock.isFillingBlock(multiPageBlockNumber)) {
                    currentNode = currentNode.getNextNode();
                    offsetFromSelectedNode--;

                    break;
                } else {
                    throw new Exception("this is totally unexpected 1");
                }
            }
        }

        do {
            DirectoryNode iteratedCurrentNode = null;
            // If the node is the selected one, update it directly so the in memory copy is uptodate.
            if (offsetFromSelectedNode == 0) {
                iteratedCurrentNode = currentNode;
                currentNode = node;
                offsetFromSelectedNode = -1;
            } else if (offsetFromSelectedNode > 0) {
                offsetFromSelectedNode--;
            }

            Iterator<MultiPageBlockHeader> multiPageBlockHeaderIterator = currentNode.getMultiPageBlockHeaders().iterator();
            for (; multiPageBlockHeaderIterator.hasNext();) {
                candidateMultiPageBlockHeader = multiPageBlockHeaderIterator.next();
                if (candidateMultiPageBlockHeader.multiPageBlockNumber == multiPageBlockNumber) {
                    if (increment) {
                        candidateMultiPageBlockHeader.incrementCount();
                    } else {
                        candidateMultiPageBlockHeader.decrementCount();
                    }
                    if (currentNode != node) {
                        currentNode.persistNodeInformation();
                    }
                    break;
                }
            }

            if (candidateMultiPageBlockHeader.inNext) {
                if (currentNode == node) {
                    currentNode = iteratedCurrentNode;
                }
                currentNode = currentNode.getNextNode();
            }
        } while (candidateMultiPageBlockHeader.inNext);
    }

    // the directory node indicates which pages move to the sibling and which remain,
    // we need to split the block and add the sibling in the right place.
    // remember the multi page block could itself be on multiple directory nodes.
    // so then when we split it we re-evaluate
    // should i just start from the first dnode
    // split the parent node
    // move the pages from median onwards to sibling Multi Page Block
    // compact the pages in the nodes Multi Page Block
    // depending on where the merge cursor is add sibling to the appropriate Multi Page Block
    // add the Multi Page Block header into the parents multiPageBlockHeaders
    MultiPageBlockHeader split(DirectoryNode node, boolean insertAfter) throws Exception {
        // Reset the offset map before doing this split.
        // This means you cannot do 2 splits at the same time.
        DirectoryNode currentNode = node;
        MultiPageBlockHeader candidateMultiPageBlockHeader = this;
        int offsetFromSelectedNode = 0;
        DirectoryNode.resetOffsetMap();
        int splittingMultiPageBlockNumber = multiPageBlockNumber;

        if (node == DirectoryNode.getRoot()) {
            throw new Exception("This should never happen as the Multi page block should be large enough to hold all of roots children.");
        }

        // find the earliest node containing pages in the multi page block indicate by the header.
        while (candidateMultiPageBlockHeader.inPrevious) {
            offsetFromSelectedNode++;
            currentNode = currentNode.getPreviousNode();

            Iterator<MultiPageBlockHeader> multiPageBlockHeaderIterator = currentNode.getMultiPageBlockHeaders().iterator();
            for (; multiPageBlockHeaderIterator.hasNext();) {
                candidateMultiPageBlockHeader = multiPageBlockHeaderIterator.next();
                if (candidateMultiPageBlockHeader.multiPageBlockNumber == splittingMultiPageBlockNumber) {
                    break;
                }
            }
        }

        MultiPageBlockHeader replacementMultiPageBlockHeader = MultiPageBlock.allocate();
        MultiPageBlockHeader splitMultiPageBlockHeader = MultiPageBlock.allocate();
        MultiPageBlockHeader returnMultiPageBlockHeader = null;
        int sumCountNodesForMultiPageBlock = 0;
        int replacementMultiPageBlockOffset = 0;
        int splitMultiPageBlockOffset = 0;
        // we will assume the split size is even so that the replacement MPB will contain exactly half
        // and the split MPB the other half of nodes.
        int splitSize = size/2;
        boolean hasPreviousFirstHalf = false;
        boolean hasNextFirstHalf = true;
        boolean hasPreviousSecondHalf = false;
        boolean hasNextSecondHalf = false;
        boolean multiPageBlockHeaderInNext;
        do {
            int subNodeOffsetForCandidate = 0;
            int multiPageBlockHeaderOffset = 0;
            DirectoryNode iteratedCurrentNode = null;

            // If the node is the selected one, update it directly so the in memory copy is uptodate.
            if (offsetFromSelectedNode == 0) {
                iteratedCurrentNode = currentNode;
                currentNode = node;
                offsetFromSelectedNode = -1;
            } else if (offsetFromSelectedNode > 0) {
                offsetFromSelectedNode--;
            }

            // ensure that the candidate multi page block header is not inadvertently mutating some data.
            // add the number of pages uptil this node corresponding to the multi page block.
            Iterator<MultiPageBlockHeader> multiPageBlockHeaderIterator = currentNode.getMultiPageBlockHeaders().iterator();
            for (; multiPageBlockHeaderIterator.hasNext(); subNodeOffsetForCandidate += candidateMultiPageBlockHeader.getCountForNode()) {
                multiPageBlockHeaderOffset++;
                candidateMultiPageBlockHeader = multiPageBlockHeaderIterator.next();
                if (candidateMultiPageBlockHeader.multiPageBlockNumber == splittingMultiPageBlockNumber) {
                    sumCountNodesForMultiPageBlock += candidateMultiPageBlockHeader.getCountForNode();
                    break;
                }
            }

            // find the sub nodes in this multi block that correspond to this header.
            // we cache the inNext value to continue the loop as the block header could be transformed by the split.
            multiPageBlockHeaderInNext = candidateMultiPageBlockHeader.inNext;
            List<Integer> subNodes = currentNode.getSubNodes().subList(subNodeOffsetForCandidate,
                    subNodeOffsetForCandidate + candidateMultiPageBlockHeader.getCountForNode());

            if (sumCountNodesForMultiPageBlock <= splitSize) {
                // we are still in the first half of the pages in the MPB.
                // so move these nodes into the replacement MPBH.
                // there will be subsequent MPBHs so in next is true.
                candidateMultiPageBlockHeader.replaceMultiPageBlock(replacementMultiPageBlockHeader.getMultiPageBlockNumber(),
                        subNodes.size(), hasPreviousFirstHalf, hasNextFirstHalf);
                // anticipating there will be atleast one more MPBH corresponding to the replacement MPB.
                hasPreviousFirstHalf = true;

                // we want to collapse the pages into first pages of the replacement multi page block.
                MultiPageBlock.copyPages(splittingMultiPageBlockNumber,
                        replacementMultiPageBlockHeader.multiPageBlockNumber,
                        subNodes, replacementMultiPageBlockOffset);
                currentNode.replaceSubNodes(subNodeOffsetForCandidate, replacementMultiPageBlockOffset, subNodes.size());

                // advance the offset after replacement.
                replacementMultiPageBlockOffset += subNodes.size();
                if (currentNode == node) {
                    returnMultiPageBlockHeader = candidateMultiPageBlockHeader;
                }
            } else {
                int remaining;
                // if there are more MPB headers after this, setup the flags accordingly.
                hasNextSecondHalf = candidateMultiPageBlockHeader.inNext;
                // if all the subnodes in the node need to be moved to the sibling MPB.
                // does this represent a partition along the middle or is fully in 2nd half.
                if (sumCountNodesForMultiPageBlock - splitSize >= subNodes.size()) {
                    remaining = subNodes.size();
                    candidateMultiPageBlockHeader.replaceMultiPageBlock(splitMultiPageBlockHeader.getMultiPageBlockNumber(),
                            subNodes.size(), hasPreviousSecondHalf, hasNextSecondHalf);

                    if (currentNode == node) {
                        returnMultiPageBlockHeader = candidateMultiPageBlockHeader;
                    }
                } else {
                    MultiPageBlockHeader copySplitMultiPageBlockHeader = splitMultiPageBlockHeader.copy();
                    remaining = sumCountNodesForMultiPageBlock - splitSize;
                    // this function does to many things and args are messed up. cleanup needed.
                    candidateMultiPageBlockHeader.insertMultiPageBlockHeader(currentNode, copySplitMultiPageBlockHeader,
                            multiPageBlockHeaderOffset, replacementMultiPageBlockHeader.getMultiPageBlockNumber(),
                            subNodes.size() - remaining, remaining, hasPreviousSecondHalf, hasNextSecondHalf);

                    if (currentNode == node) {
                        // we may need to adjust the mpbh merge cursor after the split in the mpbh.
                        currentNode.adjustMergeMultiPageBlockHeaderCursor();
                        returnMultiPageBlockHeader = currentNode.getMultiPageBlockHeaderAtCursor(0);
                    }
                }
                // This is for future MPB headers for this block.
                hasPreviousSecondHalf = true;

                // find the ones which need to be in the replacement MPB, this could be zero.
                int keep = subNodes.size() - remaining;

                if (keep > 0) {
                    MultiPageBlock.copyPages(splittingMultiPageBlockNumber, replacementMultiPageBlockHeader.multiPageBlockNumber,
                            subNodes.subList(0, keep), replacementMultiPageBlockOffset);
                    currentNode.replaceSubNodes(subNodeOffsetForCandidate, replacementMultiPageBlockOffset, keep);
                    subNodeOffsetForCandidate += keep;
                }

                MultiPageBlock.copyPages(splittingMultiPageBlockNumber, splitMultiPageBlockHeader.multiPageBlockNumber,
                        subNodes.subList(keep, subNodes.size()), splitMultiPageBlockOffset);
                currentNode.replaceSubNodes(subNodeOffsetForCandidate, splitMultiPageBlockOffset, remaining);

                replacementMultiPageBlockOffset += keep;
                splitMultiPageBlockOffset += remaining;

            }
            if (currentNode != node) {
                currentNode.persistNodeInformation();
            }
            // we should split here itself, as we can identify from which node the split will be applicable.
            if (multiPageBlockHeaderInNext) {
                if (currentNode == node) {
                    currentNode = iteratedCurrentNode;
                }
                currentNode = currentNode.getNextNode();
            }
        } while (multiPageBlockHeaderInNext);

        return returnMultiPageBlockHeader;
    }

    void replaceMultiPageBlock(int newBlockNumber, int newSize, boolean inPrevious, boolean inNext) {
        setMultiPageBlockNumber(newBlockNumber);
        setCount(size/2);
        setCountForNode(newSize);
        setInPrevious(inPrevious);
        setInNext(inNext);
    }

    void insertMultiPageBlockHeader(DirectoryNode node, MultiPageBlockHeader newMultiPageBlockHeader, int offset,
                                    int multiPageBlockNumber, int keep, int newCount, boolean inPrevious, boolean inNext) {
        setCount(size/2);
        setMultiPageBlockNumber(multiPageBlockNumber);
        setCountForNode(keep);
        // this is because the split is causing detachment between the subnodes mpbhs.
        setInNext(false);

        newMultiPageBlockHeader.setCount(size/2);
        newMultiPageBlockHeader.setCountForNode(newCount);
        newMultiPageBlockHeader.setInPrevious(inPrevious);
        newMultiPageBlockHeader.setInNext(inNext);

        node.getMultiPageBlockHeaders().add(offset, newMultiPageBlockHeader);
    }

    MultiPageBlockHeader copy() {
        return new MultiPageBlockHeader(multiPageBlockNumber, count, size, countForNode, inPrevious, inNext);
    }
}
