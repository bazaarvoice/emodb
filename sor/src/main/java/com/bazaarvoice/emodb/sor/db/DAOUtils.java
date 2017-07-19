package com.bazaarvoice.emodb.sor.db;

import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.List;

public class DAOUtils {


    public static List<ByteBuffer> getBlockedDeltas(ByteBuffer encodedDelta, int prefixLength, int deltaBlockSize) {
        List<ByteBuffer> blocks = Lists.newArrayListWithCapacity(((encodedDelta.remaining() + deltaBlockSize - 1) / deltaBlockSize) + 1);

        int originalLimit = encodedDelta.limit();

        int currentPosition = encodedDelta.position();
        int numBlocks = 0;

        while (currentPosition < originalLimit) {
            ByteBuffer split = encodedDelta.duplicate();
            int limit= Math.min(currentPosition + deltaBlockSize, split.limit());
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

        assert numBlocks < Math.pow(16, deltaBlockSize);

        byte[] blockBytes = String.format("%0" + prefixLength + "X", numBlocks).getBytes();
        for (int i = encodedDelta.position(); i < blockBytes.length ; i++) {
            encodedDelta.put(encodedDelta.position() + i, blockBytes[i]);
        }
        return blocks;
    }

    public static ByteBuffer removePrefix(ByteBuffer value, int deltaPrefixLength) {
        value.position(value.position() + deltaPrefixLength);
        return value;
    }
}
