package com.bazaarvoice.emodb.hadoop.io;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * FSDataStream implementation for an empty file.
 */
public class EmptyFSDataInputStream extends FSDataInputStream {

    public EmptyFSDataInputStream() throws IOException {
        super(new EmptyInputStream());
    }

    private static class EmptyInputStream extends InputStream implements Seekable, PositionedReadable {
        @Override
        public int read() throws IOException {
            return -1;
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos != 0) {
                throw new EOFException("Attempt to seek beyond end of file");
            }
        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            if (position != 0) {
                throw new EOFException("Attempt to read beyond end of file");
            }
            return 0;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            if (position != 0 || length != 0) {
                throw new EOFException("Attempt to read beyond end of file");
            }
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            readFully(position, buffer, 0, buffer.length);
        }
    }
}
