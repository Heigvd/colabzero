/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.colabproject.colabzero.api.store;

import ch.colabproject.colabzero.api.KafkaConfig;
import ch.colabproject.colabzero.api.store.utils.MicroserviceUtils;
import ch.colabproject.colabzero.api.store.utils.MetadataService;
import ch.colabproject.colabzero.api.store.utils.HostStoreInfo;
import static ch.colabproject.colabzero.api.rest.CardController.CALL_TIMEOUT;
import static ch.colabproject.colabzero.api.schemas.Schemas.Topics.CARDS;
import ch.colabproject.colabzero.api.avro.Card;
import fish.payara.micro.PayaraInstance;
import fish.payara.micro.data.InstanceDescriptor;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.container.AsyncResponse;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import static org.apache.kafka.streams.state.StreamsMetadata.NOT_AVAILABLE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author maxence
 */
@ApplicationScoped
public class CardStream {

    private static final Logger log = LoggerFactory.getLogger(CardStream.class);

    public static final String CARDS_STORE_NAME = "card-store";
    public static final String SERVICE_APP_ID = CardStream.class.getSimpleName();

    //  WHY ON EARTH SUCH INJECTIONS DOES NOT WORK?????
    /*@Inject
    @ConfigProperty(name = "payara.instance.http.address")
    private String address;

    @Inject
    @ConfigProperty(name = "payara.instance.http.port")
    private String port;
    
    @Inject
    private Config config;*/
    @Inject
    private PayaraInstance payara;

    private Properties props;
    private static MetadataService metadataService;
    private KafkaStreams _streams;

    //In a real implementation we would need to (a) support outstanding requests for the same Id/filter from
    // different users and (b) periodically purge old entries from this map.
    private final Map<Long, FilteredResponse<Long, Card>> outstandingRequests = new ConcurrentHashMap<>();

    public static class FilteredResponse<K, V> {

        private final AsyncResponse asyncResponse;
        private final Predicate<K, V> predicate;

        public FilteredResponse(final AsyncResponse asyncResponse, final Predicate<K, V> predicate) {
            this.asyncResponse = asyncResponse;
            this.predicate = predicate;
        }
    }

    public CardStream() {
        props = KafkaConfig.getDefaultConfig();
    }

    private KafkaStreams getStreams() {
        if (this._streams == null) {
            this._streams = this.startKStreams("localhost:29092", props);
        }
        return this._streams;
    }

    public HostStoreInfo getHostForOrderId(final Long cardId) {
        getStreams();
        return metadataService
            .streamsMetadataForStoreAndKey(CARDS_STORE_NAME, cardId, Serdes.Long().serializer());
    }

    public ReadOnlyKeyValueStore<Long, Card> getCardsStore() {
        return getStreams().store(
            StoreQueryParameters.fromNameAndType(
                CARDS_STORE_NAME,
                QueryableStoreTypes.keyValueStore()));
    }

    /**
     * Use Kafka Streams' Queryable State API to work out if a key/value pair is located on this
     * node, or on another Kafka Streams node. This returned HostStoreInfo can be used to redirect
     * an HTTP request to the node that has the data.
     * <p>
     * If metadata is available, which can happen on startup, or during a rebalance, block until it
     * is.
     */
    public HostStoreInfo getKeyLocationOrBlock(final Long id, final AsyncResponse asyncResponse) {
        HostStoreInfo locationOfKey;
        while (locationMetadataIsUnavailable(locationOfKey = getHostForOrderId(id))) {
            //The metastore is not available. This can happen on startup/rebalance.
            if (asyncResponse.isDone()) {
                //The response timed out so return
                return null;
            }
            try {
                //Sleep a bit until metadata becomes available
                Thread.sleep(Math.min(Long.parseLong(CALL_TIMEOUT), 200));
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
        return locationOfKey;
    }

    private boolean locationMetadataIsUnavailable(final HostStoreInfo hostWithKey) {
        return NOT_AVAILABLE.host().equals(hostWithKey.getHost())
            && NOT_AVAILABLE.port() == hostWithKey.getPort();
    }

    /**
     * Create a table of orders which we can query. When the table is updated we check to see if
     * there is an outstanding HTTP GET request waiting to be fulfilled.
     */
    private StreamsBuilder createCardsMaterializedView() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder
            .table(CARDS.name(),
                Consumed.with(CARDS.keySerde(), CARDS.valueSerde()),
                Materialized.as(CARDS_STORE_NAME))
            .toStream()
            .foreach(this::maybeCompleteLongPollGet);
        return builder;
    }

    private void maybeCompleteLongPollGet(final Long id, final Card card) {
        final FilteredResponse<Long, Card> callback = outstandingRequests.get(id);
        if (callback != null && callback.predicate.test(id, card)) {
            callback.asyncResponse.resume(card);
        }
    }

    public HostStoreInfo getThisHostStoreInfo() {
        InstanceDescriptor iDesc = payara.getLocalDescriptor();
        String address = iDesc.getHostName().getCanonicalHostName();
        Integer port = iDesc.getHttpPorts().get(0);

        log.info("ThisHostStoreInfo: {}:{}", address, port);

        return new HostStoreInfo(address, port);
    }

    public Map<Long, FilteredResponse<Long, Card>> getOutstandingRequests() {
        return this.outstandingRequests;
    }

    private Properties config(final String bootstrapServers, final Properties defaultConfig) {
        HostStoreInfo host = this.getThisHostStoreInfo();

        log.error("LOCALSTORE: {}", host.getPort());
        final Properties props = MicroserviceUtils.baseStreamsConfig(
            bootstrapServers,
            "/tmp/kafka-streams" + host.getPort(),
            SERVICE_APP_ID,
            defaultConfig);

        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host.getHost() + ":" + host.getPort());
        return props;
    }

    private KafkaStreams startKStreams(final String bootstrapServers,
        final Properties defaultConfig) {


        final KafkaStreams streams = new KafkaStreams(
            createCardsMaterializedView().build(),
            config(bootstrapServers, defaultConfig));


        this.metadataService = new MetadataService(streams);

        //streams.cleanUp(); //don't do this in prod as it clears your state stores
        final CountDownLatch startLatch = new CountDownLatch(1);

        streams.setStateListener((newState, oldState) -> {
            if (newState == State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                startLatch.countDown();
            }
        });
        streams.start();

        try {
            if (!startLatch.await(60, TimeUnit.SECONDS)) {
                throw new RuntimeException("Streams never finished rebalancing on startup");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return streams;
    }

    /*
     * LIFECYCLE 
     */
    @PostConstruct
    public void postConstruct() {
        log.info("Create Stream");
    }

    @PreDestroy
    public void preDestroy() {
        log.info("Destroy Stream");
    }
}
