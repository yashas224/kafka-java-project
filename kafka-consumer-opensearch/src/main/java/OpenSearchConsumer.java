import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
  private static final Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());

  public static void main(String[] args) throws IOException {
    // create open search client
    boolean isBulkRequest = true;
    RestHighLevelClient restHighLevelClient = createOpenSearchClient();
    String index = "wikimedia";
    // create kafka client
    KafkaConsumer<String, String> consumer = createKafkaConsumerClient();
    String topic = "mediawiki.recentchange";

    // shutdown hook to graciously close kafka consumer.
    final Thread mainThread = Thread.currentThread();
/*    A shutdown hook is simply an initialized but unstarted thread. When the virtual machine begins its shutdown sequence
    it will start all registered shutdown hooks in some unspecified order and let them run concurrently.
    When all the hooks have finished it will then halt. Note that daemon threads will continue to run during the shutdown sequence,
       as   will non-daemon threads if shutdown was initiated by invoking the exit method.*/
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("detected a shutdown !!!!!!");
      logger.info(" exit kafka consumer ");
      consumer.wakeup();

      try {
        mainThread.join();
        logger.info("Completed clean up for the consumer group  and closing the RestHighLevelClient from main thread !!!!");
      } catch(InterruptedException e) {
        e.printStackTrace();
      }
    }));

    // crete index
    try(restHighLevelClient; consumer) {
      // check if index exists

      GetIndexRequest getIndexRequest = new GetIndexRequest(index);
      boolean indexExists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
      logger.info("Index  {} : exists: {}", getIndexRequest.indices()[0], indexExists);

      if(!indexExists) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        logger.info("Index creation for {} : status: {}", createIndexResponse.index(), createIndexResponse.isAcknowledged());
      }

      consumer.subscribe(Collections.singleton(topic));

      while(true) {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(3000));

        logger.info("received record count {}", consumerRecords.count());

        BulkRequest bulkRequest = new BulkRequest();
        // send data to openSearch
        for(ConsumerRecord<String, String> record : consumerRecords) {
          try {
            // strategies to make the process idempotent
            // option 1- use kafka coordinates;
            String id = record.topic() + "_" + record.partition() + "_" + record.offset();
            // option 2 - using mediawiki/recentchange JSON payload field
            id = extractIdFromKafkaMessage(record.value());
            IndexRequest indexRequest = new IndexRequest(index)
               .source(record.value(), XContentType.JSON)
               .id(id);

            if(isBulkRequest) {
              bulkRequest.add(indexRequest);
            } else {
              IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
              logger.info("inserted document id {} ,  status : {} into openSearch", response.getId(), response.status());
            }
          } catch(Exception e) {
          }
        }

        if(isBulkRequest && bulkRequest.numberOfActions() > 0) {
          BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
          logger.info("inserted {} documents using bulk API ,  status : {} into openSearch", bulkResponse.getItems().length, bulkResponse.status());
        }
        commitOffsetManuallyAfterProcessingTheBatch(consumer);
      }
    } catch(WakeupException wakeupException) {
      logger.info("WakeupException triggered from the shutdown hook !!");
      consumer.close();
      restHighLevelClient.close();
    }
  }

  private static void commitOffsetManuallyAfterProcessingTheBatch(KafkaConsumer<String, String> consumer) {
    // offset committed after batch synchronous op is complete
    consumer.commitSync();
    logger.info("offset have been committed for the batch ");
  }

  private static String extractIdFromKafkaMessage(String value) {
    JsonObject jsonObject = JsonParser.parseString(value).getAsJsonObject();
    return jsonObject.get("meta").getAsJsonObject().get("id").getAsString();
  }

  private static KafkaConsumer<String, String> createKafkaConsumerClient() {
    String bootstrapServer = "127.0.0.1:9092";
    String consumerGroupId = "consumer-opensearch";
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    // committing offset manually
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return new KafkaConsumer<>(properties);
  }

  public static RestHighLevelClient createOpenSearchClient() {
    String connString = "http://localhost:9200";

    RestHighLevelClient restHighLevelClient;
    URI connUri = URI.create(connString);
    // extract login information if it exists
    String userInfo = connUri.getUserInfo();

    if(userInfo == null) {
      // REST client without security
      restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
    } else {
      // REST client with security
      String[] auth = userInfo.split(":");

      CredentialsProvider cp = new BasicCredentialsProvider();
      cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

      restHighLevelClient = new RestHighLevelClient(
         RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
            .setHttpClientConfigCallback(
               httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                  .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
    }

    return restHighLevelClient;
  }
}
