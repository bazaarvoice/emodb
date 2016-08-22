package com.bazaarvoice.emodb.hadoop.io;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

/**
 * Utility for {@link EmoFileSystem} and {@link StashFileSystem}.  Handles many of the common conversions from Paths
 * to tables and splits.
 */
public class FileSystemUtil {

    private static final FsPermission DIRECTORY_PERMISSION = new FsPermission(FsAction.READ_EXECUTE, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE);
    private static final FsPermission FILE_PERMISSION = new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ);
    private static final String EMPTY_SPLIT_FILE_NAME = "_emptySplit";

    private FileSystemUtil() {
        // empty
    }

    /**
     * Returns a FileStatus for the given root path.
     */
    public static FileStatus getRootFileStatus(Path rootPath) {
        return new FileStatus(0, true, 1, 1024, -1, -1, DIRECTORY_PERMISSION, null, null, rootPath);
    }

    /**
     * Returns a FileStatus for a table.
     */
    public static FileStatus getTableFileStatus(Path rootPath, String table) {
        return new FileStatus(0, true, 1, 1024, -1, -1, DIRECTORY_PERMISSION, null, null,
                getTablePath(rootPath, table));
    }

    /**
     * Returns a FileStatus for a split.
     */
    public static FileStatus getSplitFileStatus(Path rootPath, String table, String split, long size, int blockSize) {
        return new FileStatus(size, false, 1, blockSize, -1, -1, FILE_PERMISSION, null, null,
                getSplitPath(rootPath, table, split));
    }

    /**
     * Gets the table name from a path, or null if the path is the root path.
     */
    @Nullable
    public static String getTableName(Path rootPath, Path path) {
        path = qualified(rootPath, path);
        if (rootPath.equals(path)) {
            // Path is root, no table
            return null;
        }

        Path tablePath;
        Path parent = path.getParent();
        if (Objects.equals(parent, rootPath)) {
            // The path itself represents a table (e.g.; emodb://ci.us/mytable)
            tablePath = path;
        } else if (parent != null && Objects.equals(parent.getParent(), rootPath)) {
            // The path is a split (e.g.; emodb://ci.us/mytable/split-id)
            tablePath = parent;
        } else {
            throw new IllegalArgumentException(
                    format("Path does not represent a table, split, or root (path=%s, root=%s)", path, rootPath));
        }
        return decode(tablePath.getName());
    }

    /**
     * Gets the split from a path, or null if the path is the root or a table path.
     */
    @Nullable
    public static String getSplitName(Path rootPath, Path path) {
        path = qualified(rootPath, path);
        Path parent = path.getParent();

        if (rootPath.equals(path) || Objects.equals(rootPath, parent)) {
            // Path is root or table, no split
            return null;
        }

        if (parent == null || !Objects.equals(parent.getParent(), rootPath)) {
            throw new IllegalArgumentException(
                    format("Path does not represent a table, split, or root (path=%s, root=%s)", path, rootPath));
        }

        return decode(path.getName());
    }

    /**
     * Qualifies a path so it includes the schema and authority from the root path.
     */
    private static Path qualified(Path rootPath, Path path) {
        URI rootUri = rootPath.toUri();
        return path.makeQualified(rootUri, new Path(rootUri.getPath()));
    }

    /**
     * Gets the path for a table from the given root.
     */
    public static Path getTablePath(Path rootPath, String table) {
        return new Path(rootPath, encode(table));
    }

    /**
     * Gets the path for a split from the given root.
     */
    public static Path getSplitPath(Path rootPath, String table, String split) {
        return new Path(getTablePath(rootPath, table), encode(split));
    }

    /**
     * Hive tries to substitute in empty files when a file has no splits.  These empty files do not come from an
     * EmoFileSystem or StashFileSystem, such as LocalFileSystem.  To avert this behavior always return at least one
     * split file.  If this split is empty then the FileSystem should return a single path with a split named
     * from this method's returned value.
     */
    public static String getEmptySplitFileName() {
        return EMPTY_SPLIT_FILE_NAME;
    }

    public static boolean isEmptySplit(Path path) {
        return path.getName().equals(EMPTY_SPLIT_FILE_NAME);
    }

    public static boolean isEmptySplit(String fileName) {
        return fileName.equals(EMPTY_SPLIT_FILE_NAME);
    }

    public static BaseRecordReader getEmptySplitRecordReader() {
        return new BaseRecordReader(1) {
            @Override
            protected Iterator<Map<String, Object>> getRowIterator()
                    throws IOException {
                return Iterators.emptyIterator();
            }

            @Override
            protected void closeOnce()
                    throws IOException {
                // Do nothing
            }
        };
    }

    /**
     * URL encodes a path element
     */
    private static String encode(String pathElement) {
        try {
            return URLEncoder.encode(pathElement, Charsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e); // Should never happen
        }
    }

    /**
     * URL decodes a path element
     */
    private static String decode(String pathElement) {
        try {
            return URLDecoder.decode(pathElement, Charsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e); // Should never happen
        }
    }
}
