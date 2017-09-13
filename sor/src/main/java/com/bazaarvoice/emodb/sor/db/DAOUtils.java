package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.sor.db.astyanax.BlockSize;
import com.bazaarvoice.emodb.sor.db.astyanax.PrefixLength;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.nio.ByteBuffer;
import java.util.List;

public class DAOUtils {

    private final int _prefixLength;
    private final int _deltaBlockSize;
    private final byte[] _singleBlockBytes;

    @Inject
    public DAOUtils(@PrefixLength  int prefixLength, @BlockSize int deltaBlockSize) {
        _prefixLength = prefixLength;
        _deltaBlockSize = deltaBlockSize;
        _singleBlockBytes = String.format("%0" + prefixLength + "X", 1).getBytes();
    }

    public List<ByteBuffer> getDeltaBlocks(ByteBuffer encodedDelta) {
        List<ByteBuffer> blocks = Lists.newArrayListWithCapacity(((encodedDelta.remaining() + _deltaBlockSize - 1) / _deltaBlockSize) + 1);

        int originalLimit = encodedDelta.limit();

        int currentPosition = encodedDelta.position();
        int numBlocks = 0;

        while (currentPosition < originalLimit) {
            ByteBuffer split = encodedDelta.duplicate();
            int limit= Math.min(currentPosition + _deltaBlockSize, split.limit());
            if (limit < split.limit()) {
                // if current limit is in the middle of the utf-8 character, then backtrack to the beginning of said character
                while ((encodedDelta.get(limit) & 0x80) != 0 && (encodedDelta.get(limit) & 0x40) == 0) {
                    limit--;
                }
                split.limit(limit);
            }
            split.position(currentPosition);
            currentPosition = limit;
            blocks.add(split);
            numBlocks++;
        }

        assert numBlocks < Math.pow(16, _deltaBlockSize);

        byte[] blockBytes = numBlocks == 1 ? _singleBlockBytes : String.format("%0" + _prefixLength + "X", numBlocks).getBytes();
        for (int i = encodedDelta.position(); i < blockBytes.length ; i++) {
            encodedDelta.put(encodedDelta.position() + i, blockBytes[i]);
        }
        return blocks;
    }

    // removes the hex prefix that indicates the number of blocks in the delta
    public ByteBuffer skipPrefix(ByteBuffer value) {
        value.position(value.position() + _prefixLength);
        return value;
    }
}