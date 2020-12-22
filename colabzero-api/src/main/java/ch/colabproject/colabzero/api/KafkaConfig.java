/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.colabproject.colabzero.api;

import ch.colabproject.colabzero.api.schemas.Schemas;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import java.util.Properties;

/**
 *
 * @author maxence
 */
public final class KafkaConfig {

    private static boolean firstTime = true;

    private KafkaConfig() {
        // STATIC CLASS
    }

    public static Properties getDefaultConfig() {
        Properties props = new Properties();

        props.put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        if (firstTime) {
            Schemas.configureSerdes(props);
            firstTime = false;
        }

        return props;
    }
}
