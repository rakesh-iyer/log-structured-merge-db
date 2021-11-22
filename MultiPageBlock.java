import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
    static final String MULTI_PAGE_BLOCK_FILE_SUFFIX = ".mpb";

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

    static MultiPageBlock get(MultiPageBlockHeader multiPageBlockHeader) throws Exception {
        return get(multiPageBlockHeader.getMultiPageBlockNumber());
    }

    // this may need locking if file is open for reading.
    void write(MultiPageBlockHeader multiPageBlockHeader) throws IOException {
        String multiPageBlockFileName = multiPageBlockHeader.getMultiPageBlockNumber() + ".mpb";
        FileChannel fileChannel = FileChannel.open(Paths.get(multiPageBlockFileName), StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        ByteBuffer activePagesBuffer = ByteBuffer.allocate(Integer.BYTES);
        activePagesBuffer.putInt(activePages).flip();
        fileChannel.write(activePagesBuffer);

        blockBB.clear();
        // This may not write all the bytes, so we need to make this a synch call.
        fileChannel.write(blockBB);
        fileChannel.force(true);
        fileChannel.close();
    }

    static String getMultiPageBlockFile(int multiPageBlockNumber) {
        return multiPageBlockNumber + MULTI_PAGE_BLOCK_FILE_SUFFIX;
    }

    static MultiPageBlock get(int multiPageBlockNumber) throws Exception {
        MultiPageBlock multiPageBlock = inMemoryMultiPageBlocks.get(multiPageBlockNumber);
        if (multiPageBlock != null) {
            return multiPageBlock;
        }

        multiPageBlock = new MultiPageBlock();
        FileChannel fileChannel = FileChannel.open(Paths.get(getMultiPageBlockFile(multiPageBlockNumber)), StandardOpenOption.READ);

        // Make the block ready for reading.
        ByteBuffer intBuffer = ByteBuffer.allocate(4);
        intBuffer.mark();
        fileChannel.read(intBuffer);
        intBuffer.reset();
        logger.info(intBuffer);
        multiPageBlock.setActivePages(intBuffer.getInt());

        multiPageBlock.blockBB.clear();
        // This may not read all the bytes, so we need to make this a synch call.
        fileChannel.read(multiPageBlock.blockBB);
        inMemoryMultiPageBlocks.put(multiPageBlockNumber, multiPageBlock);

        fileChannel.close();
        return multiPageBlock;
    }

    ByteBuffer getPageBuffer(int pageNumber) {
        return ByteBuffer.wrap(blockBB.array(), pageNumber * Node.PAGE_SIZE, Node.PAGE_SIZE);
    }

    static void copyPages(int srcMultiPageBlockNumber, int destMultiPageBlockNumber, List<Integer> pageNumbers, int destinationMultiPageBlockOffset) throws Exception {
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
