package com.bazaarvoice.emodb.web.resources.uac;

import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.CreateEmoApiKeyResponse;
import com.bazaarvoice.emodb.uac.api.EmoApiKey;
import com.bazaarvoice.emodb.uac.api.MigrateEmoApiKeyRequest;
import com.bazaarvoice.emodb.uac.api.UpdateEmoApiKeyRequest;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.bazaarvoice.emodb.web.uac.SubjectUserAccessControl;
import com.google.inject.Inject;
import io.swagger.annotations.Api;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.authz.annotation.RequiresPermissions;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Resource for API key management.  Note that the usual {@link RequiresPermissions} annotations aren't used on this
 * resource.  Because permissions for API key management are so granular they are fully enforced by the contained
 * SubjectUserAccessControl, so adding permission checking annotations here would be redundant.
 */
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "API Key: ", description = "All API key management operations")
@RequiresAuthentication
public class ApiKeyResource1 {

    private final SubjectUserAccessControl _uac;

    @Inject
    public ApiKeyResource1(SubjectUserAccessControl uac) {
        _uac = checkNotNull(uac, "uac");
    }

    /**
     * RESTful endpoint for viewing an API key.
     */
    @GET
    @Path("{id}")
    public EmoApiKey getApiKey(@PathParam("id") String id, @Authenticated Subject subject) {
        return _uac.getApiKey(subject, id);
    }

    /**
     * Gets an API key by its key
     */
    @GET
    @Path("_key/{key}")
    public EmoApiKey getApiKeyByKey(@PathParam("key") String key, @Authenticated Subject subject) {
        return _uac.getApiKeyByKey(subject, key);
    }

    /**
     * Creates an API Key.
     */
    @POST
    @Consumes("application/x.json-create-api-key")
    public Response createApiKey(CreateEmoApiKeyRequest request, @QueryParam("key") String key,
                                 @Authenticated Subject subject) {
        if (key != null) {
            request.setCustomRequestParameter("key", key);
        }
        CreateEmoApiKeyResponse response = _uac.createApiKey(subject, request);

        return Response.created(URI.create(response.getId()))
                .entity(response)
                .build();
    }

    /**
     * RESTful endpoint for updating an API key.  Note that all attributes of the key will be updated to match the
     * provided object.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{id}")
    public SuccessResponse updateApiKey(@PathParam("id") String id, EmoApiKey emoApiKey,
                                        @Authenticated Subject subject) {
        checkArgument(emoApiKey.getId().equals(id), "Body contains conflicting API key identifier");
        return updateApiKey(id,
                new UpdateEmoApiKeyRequest()
                    .setOwner(emoApiKey.getOwner())
                    .setDescription(emoApiKey.getDescription())
                    .setAssignedRoles(emoApiKey.getRoles())
                    .setUnassignOtherRoles(true),
                subject);
    }

    /**
     * Alternate endpoint for updating an API key.  Although not REST compliant it provides a more flexible
     * interface for specifying API key attributes, such as by supporting partial updates.
     */
    @PUT
    @Consumes("application/x.json-update-api-key")
    @Path("{id}")
    public SuccessResponse updateApiKey(@PathParam("id") String id, UpdateEmoApiKeyRequest request,
                                        @Authenticated Subject subject) {
        _uac.updateApiKey(subject, request.setId(id));
        return SuccessResponse.instance();
    }

    /**
     * Migrates an API key to a new private key.
     */
    @POST
    @Path("{id}/migrate")
    public CreateEmoApiKeyResponse migrateApiKey(@PathParam("id") String id, @QueryParam("key") String key,
                                                 @Authenticated Subject subject) {
        MigrateEmoApiKeyRequest request = new MigrateEmoApiKeyRequest(id);
        if (key != null) {
            request.setCustomRequestParameter("key", key);
        }
        String newKey = _uac.migrateApiKey(subject, request);
        // Even though we're technically not creating a new API key the response object is the same
        return new CreateEmoApiKeyResponse(newKey, id);
    }

    /**
     * RESTful endpoint for deleting an API key.
     */
    @DELETE
    @Path("{id}")
    public SuccessResponse deleteApiKey(@PathParam("id") String id, @Authenticated Subject subject) {
        _uac.deleteApiKey(subject, id);
        return SuccessResponse.instance();
    }
}
