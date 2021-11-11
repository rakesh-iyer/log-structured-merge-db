import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Getter @Setter
public class MultiPageBlock extends Debuggable {
    static Logger logger = Logger.getLogger(MultiPageBlock.class);
    final static int NUMBER_OF_PAGES = 32;
    final static int MULTI_PAGE_BLOCK_SIZE = Node.PAGE_SIZE * NUMBER_OF_PAGES;
    // why re-entrant is the most suitable option here?
    Lock fileLock = new ReentrantLock();
    int activePages;
    static Map<Integer, MultiPageBlock> inMemoryMultiPageBlocks = new HashMap<>();

    // use a byte buffer so leaf nodes can use views of this.
    ByteBuffer blockBB = ByteBuffer.allocate(MULTI_PAGE_BLOCK_SIZE);

    static MultiPageBlockHeader allocate() {
        MultiPageBlock multiPageBlock = new MultiPageBlock();
        MultiPageBlockHeader multiPageBlockHeader = MultiPageBlockHeader.allocate(0, NUMBER_OF_PAGES, 0);
        inMemoryMultiPageBlocks.put(multiPageBlockHeader.getMultiPageBlockNumber(), multiPageBlock);

        return multiPageBlockHeader;
    }

    boolean isFull() {
      return activePages == NUMBER_OF_PAGES;
    }

    void decrementActivePages() {
        activePages--;
    }

    void incrementActivePages() {
        activePages++;
    }

    static MultiPageBlock get(MultiPageBlockHeader multiPageBlockHeader) {
        return inMemoryMultiPageBlocks.get(multiPageBlockHeader.getMultiPageBlockNumber());
    }

    static MultiPageBlock get(int multiPageBlockNumber) {
        return inMemoryMultiPageBlocks.get(multiPageBlockNumber);
    }

    // this may need locking if file is open for reading.
    void write(MultiPageBlockHeader multiPageBlockHeader) throws IOException {
        String multiPageBlockFileName =  multiPageBlockHeader.getMultiPageBlockNumber() + ".mpb";
        FileChannel fileChannel = FileChannel.open(Paths.get(multiPageBlockFileName));

        fileChannel.write(blockBB);
        fileChannel.close();
    }

    static MultiPageBlock read(MultiPageBlockHeader multiPageBlockHeader) throws IOException {
        MultiPageBlock multiPageBlock = inMemoryMultiPageBlocks.get(multiPageBlockHeader.getMultiPageBlockNumber());
        if (multiPageBlock != null) {
            return multiPageBlock;
        }

        multiPageBlock = new MultiPageBlock();
        String multiPageBlockFileName =  multiPageBlockHeader.getMultiPageBlockNumber() + ".mpb";
        FileChannel fileChannel = FileChannel.open(Paths.get(multiPageBlockFileName));

        // Make the block ready for reading.
        multiPageBlock.blockBB.clear();
        fileChannel.read(multiPageBlock.blockBB);
        inMemoryMultiPageBlocks.put(multiPageBlockHeader.getMultiPageBlockNumber(), multiPageBlock);

        fileChannel.close();
        return multiPageBlock;
    }

    ByteBuffer getPageBuffer(int pageNumber) {
        return ByteBuffer.wrap(blockBB.array(), pageNumber * Node.PAGE_SIZE, Node.PAGE_SIZE);
    }

    static void copyPages(int srcMultiPageBlockNumber, int destMultiPageBlockNumber, List<Integer> pageNumbers, int destinationMultiPageBlockOffset) {
        MultiPageBlock srcMultiPageBlock = MultiPageBlock.get(srcMultiPageBlockNumber);
        MultiPageBlock destMultiPageBlock = MultiPageBlock.get(destMultiPageBlockNumber);

        for (int i = 0; i < pageNumbers.size(); i++) {
            logger.info("Copying multipage block " + srcMultiPageBlock + ":" + srcMultiPageBlockNumber + " at page " + pageNumbers.get(i) +
                    "to multi page block " + destMultiPageBlock + ":" + destMultiPageBlockNumber + " at page " + (i + destinationMultiPageBlockOffset));

            ByteBuffer destByteBuffer = destMultiPageBlock.getPageBuffer(i + destinationMultiPageBlockOffset);
            destByteBuffer.put(srcMultiPageBlock.getPageBuffer(pageNumbers.get(i)));

            destMultiPageBlock.incrementActivePages();
        }
    }

    static int getMultiPageBlockNumber(MultiPageBlock multiPageBlock) throws Exception{
        for (Map.Entry<Integer, MultiPageBlock> entry: inMemoryMultiPageBlocks.entrySet()) {
            if (entry.getValue() == multiPageBlock) {
                return entry.getKey();
            }
        }

        throw new Exception("Unexpected no match in search for mpb. " + multiPageBlock);
    }

    // for debugging
    static boolean isFillingBlock(int blockNumber) throws Exception {
        for (MultiPageBlock multiPageBlock: LSM2Level.levelMerge.fillingBlocks) {
            int multiPageBlockNumber = MultiPageBlock.getMultiPageBlockNumber(multiPageBlock);

            if (multiPageBlockNumber == blockNumber) {
                return true;
            }
        }

        return false;
    }
}
