package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.sor.db.astyanax.BlockSize;
import com.bazaarvoice.emodb.sor.db.astyanax.PrefixLength;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class DAOUtils {

    private final int _prefixLength;
    private final int _deltaBlockSize;
    private final byte[] _singleBlockBytes;
    private final int _maxBlocks;

    @Inject
    public DAOUtils(@PrefixLength  int prefixLength, @BlockSize int deltaBlockSize) {
        checkArgument(prefixLength > 0, "Prefix length must be greater than 0");
        checkArgument(deltaBlockSize > 0, "Delta block size must be greater than 0");

        _prefixLength = prefixLength;
        _deltaBlockSize = deltaBlockSize;
        _singleBlockBytes = String.format("%0" + prefixLength + "X", 1).getBytes();

        // this is the maximum number that can be using hexidecimal using the # of digits equal to prefix length
        _maxBlocks = (int) Math.pow(16, _prefixLength) - 1;
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

        checkArgument(numBlocks <= _maxBlocks, "Delta is too large, as it has exceeded to maximum number of blocks");

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