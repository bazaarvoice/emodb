package com.bazaarvoice.emodb.databus.client2;

import com.bazaarvoice.emodb.databus.client2.Json.DocumentMetadataDeserializer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.Serializable;
import java.util.Objects;

/**
 * Basic metadata about a document in EmoDB.  Specifically, the subset of intrinsics required to efficiently store,
 * query, and build Stash data without needing to re-parse them from the original JSON document.
 */
@JsonDeserialize(using = DocumentMetadataDeserializer.class)
public class DocumentMetadata implements Serializable {

    private final DocumentId _documentId;
    private final DocumentVersion _documentVersion;
    private final boolean _deleted;

    @JsonCreator
    public DocumentMetadata(@JsonProperty("documentId") DocumentId documentId,
                            @JsonProperty("documentVersion") DocumentVersion documentVersion,
                            @JsonProperty("deleted") boolean deleted) {
        _documentId = documentId;
        _documentVersion = documentVersion;
        _deleted = deleted;
    }

    public DocumentId getDocumentId() {
        return _documentId;
    }

    public DocumentVersion getDocumentVersion() {
        return _documentVersion;
    }

    public boolean isDeleted() {
        return _deleted;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentMetadata)) {
            return false;
        }

        DocumentMetadata that = (DocumentMetadata) o;

        return Objects.equals(_documentId, that._documentId) && Objects.equals(_documentVersion, that._documentVersion)
                && _deleted == that._deleted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_documentId, _documentVersion, _deleted);
    }
}
