package com.bazaarvoice.emodb.web.resources.uac;

import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.uac.api.CreateEmoRoleRequest;
import com.bazaarvoice.emodb.uac.api.EmoRole;
import com.bazaarvoice.emodb.uac.api.EmoRoleKey;
import com.bazaarvoice.emodb.uac.api.UpdateEmoRoleRequest;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.bazaarvoice.emodb.web.uac.SubjectUserAccessControl;
import com.google.common.collect.Lists;
import io.swagger.annotations.Api;
import java.util.List;
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
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Resource for role management.  Note that the usual {@link RequiresPermissions} annotations aren't used on this
 * resource.  Because permissions for role management are so granular they are fully enforced by the contained
 * SubjectUserAccessControl, so adding permission checking annotations here would be redundant.
 */
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Role: ", description = "All role management operations")
@RequiresAuthentication
public class RoleResource1 {

    private final SubjectUserAccessControl _uac;

    public RoleResource1(SubjectUserAccessControl uac) {
        _uac = checkNotNull(uac, "uac");
    }

    /**
     * Returns all roles for which the caller has read access.  Since the number of roles is typically low
     * this call does not support "from" or "limit" parameters similar to the system or record.
     */
    @GET
    public List<EmoRole> getAllRoles(final @Authenticated Subject subject) {
        return Lists.newArrayList(_uac.getAllRoles(subject));
    }

    /**
     * Returns all roles in the specified group for which the caller has read access.  Since the number of roles is
     * typically low this call does not support "from" or "limit" parameters similar to the system or record.
     */
    @GET
    @Path("{group}")
    public List<EmoRole> getAllRolesInGroup(@PathParam("group") String group,
                                                final @Authenticated Subject subject) {
        return Lists.newArrayList(_uac.getAllRolesInGroup(subject, group));
    }

    /**
     * RESTful endpoint for viewing a role.
     */
    @GET
    @Path("{group}/{id}")
    public EmoRole getRole(@PathParam("group") String group, @PathParam("id") String id,
                           final @Authenticated Subject subject) {
        return _uac.getRole(subject, new EmoRoleKey(group, id));
    }

    /**
     * RESTful endpoint for creating a role.
     */
    @POST
    @Path("{group}/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createRole(@PathParam("group") String group, @PathParam("id") String id, EmoRole role,
                                      final @Authenticated Subject subject) {
        checkArgument(role.getId().equals(new EmoRoleKey(group, id)),
                "Body contains conflicting role identifier");
        return createRoleFromUpdateRequest(group, id,
                new CreateEmoRoleRequest()
                        .setName(role.getName())
                        .setDescription(role.getDescription())
                        .setPermissions(role.getPermissions()),
                subject);
    }

    /**
     * Alternate endpoint for creating a role.  Although slightly less REST compliant it provides a more flexible
     * interface for specifying role attributes.
     */
    @POST
    @Path("{group}/{id}")
    @Consumes("application/x.json-create-role")
    public Response createRoleFromUpdateRequest(@PathParam("group") String group, @PathParam("id") String id,
                                                CreateEmoRoleRequest request,
                                                @Authenticated Subject subject) {
        _uac.createRole(subject, request.setRoleKey(new EmoRoleKey(group, id)));
        return Response.created(URI.create(""))
                .entity(SuccessResponse.instance())
                .build();
    }

    /**
     * RESTful endpoint for updating a role.  Note that all attributes of the role will be updated to match the
     * provided object.
     */
    @PUT
    @Path("{group}/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    public SuccessResponse updateRole(@PathParam("group") String group, @PathParam("id") String id,
                                      EmoRole role, final @Authenticated Subject subject) {
        checkArgument(role.getId().equals(new EmoRoleKey(group, id)),
                "Body contains conflicting role identifier");

        return updateRoleFromUpdateRequest(group, id,
                new UpdateEmoRoleRequest()
                        .setName(role.getName())
                        .setDescription(role.getDescription())
                        .setGrantedPermissions(role.getPermissions())
                        .setRevokeOtherPermissions(true),
                subject);
    }

    /**
     * Alternate endpoint for updating a role.  Although not REST compliant it provides a more flexible
     * interface for specifying role attributes, such as by supporting partial updates.
     */
    @PUT
    @Path("{group}/{id}")
    @Consumes("application/x.json-update-role")
    public SuccessResponse updateRoleFromUpdateRequest(@PathParam("group") String group, @PathParam("id") String id,
                                                       UpdateEmoRoleRequest request, @Authenticated Subject subject) {
        _uac.updateRole(subject, request.setRoleKey(new EmoRoleKey(group, id)));
        return SuccessResponse.instance();
    }

    /**
     * RESTful endpoint for deleting a role.
     */
    @DELETE
    @Path("{group}/{id}")
    public SuccessResponse deleteRole(@PathParam("group") String group, @PathParam("id") String id,
                                      final @Authenticated Subject subject) {
        _uac.deleteRole(subject, new EmoRoleKey(group, id));
        return SuccessResponse.instance();
    }

    @GET
    @Path("{group}/{id}/permitted")
    public boolean checkPermission(@PathParam("group") String group, @PathParam("id") String id,
                                   @QueryParam("permission") String permission, @Authenticated Subject subject) {
        return _uac.checkRoleHasPermission(subject, new EmoRoleKey(group, id), permission);
    }
}
