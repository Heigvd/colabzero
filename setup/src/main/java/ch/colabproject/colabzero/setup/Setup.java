/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ch.colabproject.colabzero.setup;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;

/**
 *
 * @author maxence
 */
public class Setup {

    public static void createTopic(AdminClient client){
        NewTopic newTopic = new NewTopic("microchanges", 1, (short) 1);
    }

    public static void main(String... args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "tralala");

        AdminClient client = AdminClient.create(props);

        try {
            System.out.println("Nodes");
            DescribeClusterResult cluster = client.describeCluster();
            KafkaFuture<Collection<Node>> nodes = cluster.nodes();
            Collection<Node> theNodes = nodes.get();
            for (Node node : theNodes) {
                System.out.println("Node: ");
                System.out.println("Node: " + node);
            }
        } catch (ExecutionException | InterruptedException ex) {
            System.err.println("Exception: " + ex);
            ex.printStackTrace();
        }
    }
}
