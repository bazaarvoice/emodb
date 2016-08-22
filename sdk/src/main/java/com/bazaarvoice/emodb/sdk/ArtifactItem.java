package com.bazaarvoice.emodb.sdk;

import com.google.common.base.Objects;
import org.apache.maven.plugins.annotations.Parameter;
import org.eclipse.aether.resolution.ArtifactResult;

public final class ArtifactItem {

    @Parameter(required = true)
    private String groupId;

    @Parameter(required = true)
    private String artifactId;

    @Parameter(required = true)
    private String version = null;

    @Parameter
    private String type = "jar";

    @Parameter
    private String classifier = "";

    /** Will be set by resolution code. */
    private ArtifactResult resolvedArtifact;

    public boolean isSnapshot() {
        return version.endsWith("-SNAPSHOT");
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public void setArtifactId(String artifactId) {
        this.artifactId = artifactId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getClassifier() {
        return classifier;
    }

    public void setClassifier(String classifier) {
        this.classifier = classifier;
    }

    /*package*/ ArtifactResult getResolvedArtifact() {
        return resolvedArtifact;
    }

    /*package*/ void setResolvedArtifact(ArtifactResult resolvedArtifact) {
        this.resolvedArtifact = resolvedArtifact;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).
                add("groupId", groupId).
                add("artifactId", artifactId).
                add("version", version).
                add("type", type).
                add("classifier", classifier).
                add("resolvedArtifact", resolvedArtifact).
                toString();
    }
}
