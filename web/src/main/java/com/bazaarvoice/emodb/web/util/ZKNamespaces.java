package com.bazaarvoice.emodb.web.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

public class ZKNamespaces {

    public static CuratorFramework usingChildNamespace(CuratorFramework curator, String... children) {
        String path = curator.getNamespace();
        for (String child : children) {
            path = path.length() > 0 ? ZKPaths.makePath(path, child) : child;
        }
        // Curator does not allow namespaces to start with a leading '/'
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return curator.usingNamespace(path);
    }
}
