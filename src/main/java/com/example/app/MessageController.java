package com.example.app;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/api/messages")
public class MessageController {

    @Inject
    MessageService messageService;

    @POST
    @Path("/send")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String send(MessageRequest request) {
        try {
            var duration = messageService.sendMessages(
                request.messageCount(),
                request.externalPayload(),
                request.useFlink(),
                request.payloadSize()
            );
            return "{'duration': '" + duration + "'}";
        } catch (Exception e) {
            return "{'error': '" + e.getMessage() + "'}";
        }
    }
}
