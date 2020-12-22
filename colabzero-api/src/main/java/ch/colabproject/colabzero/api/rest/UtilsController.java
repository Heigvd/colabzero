/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.colabproject.colabzero.api.rest;

import ch.colabproject.colabzero.api.store.CardStream;
import ch.colabproject.colabzero.api.store.utils.HostStoreInfo;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author maxence
 */
@Path("utils")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class UtilsController {

    private static final Logger log = LoggerFactory.getLogger(UtilsController.class);

    @Inject
    private CardStream cardStream;

    @GET
    @Path("HostInfo")
    public HostStoreInfo getHostInfo() {
        return cardStream.getThisHostStoreInfo();
    }
}
