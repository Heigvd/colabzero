/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.colabproject.colabzero.api.rest;

import ch.colabproject.colabzero.api.store.CardProducer;
import ch.colabproject.colabzero.api.store.CardStream;
import ch.colabproject.colabzero.api.store.CardStream.FilteredResponse;
import ch.colabproject.colabzero.api.store.utils.HostStoreInfo;
import ch.colabproject.colabzero.api.store.utils.MetadataService;
import ch.colabproject.colabzero.api.store.utils.MicroserviceUtils;
import ch.colabproject.colabzero.api.avro.Card;
import ch.colabproject.colabzero.api.bean.CardBean;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author maxence
 */
@Path("cards")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class CardController {

    private final Client client = ClientBuilder.newBuilder().build();

    private static final Logger log = LoggerFactory.getLogger(CardController.class);
    public static final String CALL_TIMEOUT = "10000";

    @Inject
    private CardStream cardStream;

    @Inject
    private CardProducer cardProducer;

    /* *********************** API ********************** */
    /**
     *
     * @return
     */
    @GET
    public List<CardBean> index() {
        log.info("GET /");
        KeyValueIterator<Long, Card> all = cardStream.getCardsStore().all();
        List<CardBean> cards = new ArrayList<>();

        while (all.hasNext()) {
            cards.add(CardBean.toBean(all.next().value));
        }
        //return cardStream.getCards();
        return cards;
    }

    private String pathFor(HttpServletRequest request, HostStoreInfo hostInfo) {
        String baseUrl = "http://" + hostInfo.getHost() + ":" + hostInfo.getPort();

        return baseUrl + request.getRequestURI() + "?" + request.getQueryString();
    }

    @GET
    @Path("/{id}")
    @ManagedAsync
    public void getCard(@PathParam("id") Long id,
        @QueryParam("timeout") @DefaultValue(CALL_TIMEOUT) final Long timeout,
        @Context HttpServletRequest request,
        @Suspended final AsyncResponse asyncResponse) {

        log.info("GET /{}", id);
        MicroserviceUtils.setTimeout(timeout, asyncResponse);

        final HostStoreInfo hostForKey = cardStream.getKeyLocationOrBlock(id, asyncResponse);

        if (hostForKey == null) { //request timed out so return
            return;
        }
        //Retrieve the card locally or reach out to a different instance if the required partition is hosted elsewhere.
        if (this.thisHost(hostForKey)) {
            fetchLocal(id, asyncResponse, (k, v) -> k.equals(v.getId()));
        } else {
            final String path = pathFor(request, hostForKey);
            this.fetchFromOtherHost(path, asyncResponse, CardBean.class);
        }
    }

    @POST
    @ManagedAsync
    public void createCard(CardBean card,
        @Suspended final AsyncResponse response) {
        log.info("Post /{}", card);
        cardProducer.postCard(card.fromBean(), callback(response, card.getId()));
    }

    @PUT
    @Path("/{id}")
    @ManagedAsync
    public void updateCard(@PathParam("id") Long id, CardBean card,
        @Suspended final AsyncResponse response) {
        log.info("Put /{}/{}",id, card);
        cardProducer.putCard(id, card.fromBean(), callback(response, card.getId()));
    }

    @DELETE
    @Path("/{id}")
    @ManagedAsync
    public void deleteCard(@PathParam("id") Long id,
        @Suspended final AsyncResponse response) {
        log.info("DELETE /{}", id);
        cardProducer.deleteCard(id, callback(response, id));
    }

    private Callback callback(final AsyncResponse response, final long cardId) {
        return (recordMetadata, e) -> {
            if (e != null) {
                response.resume(e);
            } else {
                try {
                    //Return the location of the newly created resource
                    final Response uri = Response.created(new URI("/api/cards/" + cardId)).build();
                    response.resume(uri);
                } catch (final URISyntaxException e2) {
                    e2.printStackTrace();
                }
            }
        };
    }

    private boolean thisHost(final HostStoreInfo host) {
        HostStoreInfo localHost = cardStream.getThisHostStoreInfo();
        return localHost.equals(host);
    }

    /**
     * Fetch the card from the local materialized view
     *
     * @param id            ID to fetch
     * @param asyncResponse the response to call once completed
     * @param predicate     a filter that for this fetch, so for example we might fetch only
     *                      VALIDATED orders
     */
    private void fetchLocal(final Long id, final AsyncResponse asyncResponse, final Predicate<Long, Card> predicate) {
        log.info("running GET on this node");
        try {
            final Card card = cardStream.getCardsStore().get(id);
            if (card == null || !predicate.test(id, card)) {
                log.info("Delaying get as order not present for id " + id);
                cardStream.getOutstandingRequests().put(id, new FilteredResponse<>(asyncResponse, predicate));
            } else {
                asyncResponse.resume(CardBean.toBean(card));
            }
        } catch (final InvalidStateStoreException e) {
            //Store not ready so delay
            cardStream.getOutstandingRequests().put(id, new FilteredResponse<>(asyncResponse, predicate));
        }
    }

    private <T> void fetchFromOtherHost(final String path,
        final AsyncResponse asyncResponse,
        Class<T> klass) {

        log.info("Chaining GET {} => {} to a different instance" + path, klass.getSimpleName());
        try {
            final T bean = client.target(path)
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get(klass);
            asyncResponse.resume(bean);
        } catch (final Exception swallowed) {
            log.warn("GET failed.", swallowed);
        }
    }
}
