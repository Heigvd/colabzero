/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.colabproject.colabzero.api.store;

import ch.colabproject.colabzero.api.KafkaConfig;
import ch.colabproject.colabzero.api.schemas.Schemas;
import ch.colabproject.colabzero.api.avro.Card;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import java.util.Properties;
import java.util.concurrent.Future;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.RequestScoped;
import javax.transaction.TransactionScoped;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author maxence
 */
@ApplicationScoped
public class CardProducer {

    private Producer<Long, Card> cardProducer;

    private Properties props;

    public CardProducer() {
        props = KafkaConfig.getDefaultConfig();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "colabzero-card-sender");

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
    }

    private Producer<Long, Card> getProducer() {
        if (cardProducer == null) {
            cardProducer = new KafkaProducer<>(props,
                Schemas.Topics.CARDS.keySerde().serializer(),
                Schemas.Topics.CARDS.valueSerde().serializer()
            );
        }
        return cardProducer;
    }

    public Future<RecordMetadata> postCard(Card card, Callback callback) {
        ProducerRecord<Long, Card> record = new ProducerRecord<>(Schemas.Topics.CARDS.name(), card.getId(), card);
        return getProducer().send(record, callback);
    }

    public Future<RecordMetadata> putCard(Long id, Card card, Callback callback) {
        ProducerRecord<Long, Card> record = new ProducerRecord<>(Schemas.Topics.CARDS.name(), id, card);
        return getProducer().send(record, callback);
    }

    public Future<RecordMetadata> deleteCard(Long id, Callback callback) {
        ProducerRecord<Long, Card> record = new ProducerRecord<>(Schemas.Topics.CARDS.name(), id, null);
        return getProducer().send(record, callback);
    }
}
