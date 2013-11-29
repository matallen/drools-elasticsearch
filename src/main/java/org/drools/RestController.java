package org.drools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;

@Path("/events")
public class RestController {
	
  @GET
  @Path("/new/{component}/{event}")
  public Response newEvent(@PathParam("component") String component, @PathParam("event") String eventName) {
//  	Event event=new Event(component, eventName);
//  	getEventProcessor(true).insertEvent(event);
//  	System.out.println("newEvent [component='"+component+"', event='"+eventName+"']");
  	return Response.status(200).entity("newEvent [component='"+component+"', event='"+eventName+"'] ").build();
  }
}
