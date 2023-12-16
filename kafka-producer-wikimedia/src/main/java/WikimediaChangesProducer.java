import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
  public static void main(String[] args) throws InterruptedException {

    // producer properties
    String topic = "mediawiki.recentchange";
    String bootstrapServer = "127.0.0.1:9092";
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // setting safe kafka producers of kafka clients <=2.8
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));

    // high throughput producer configs , adding compression of batches
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

    // create producer client
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

//    EventSource
    EventHandler eventHandler = new WikimediaChangeHandler(kafkaProducer, topic);
    String sseUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

    EventSource.Builder eventSourceBuilder = new EventSource.Builder(eventHandler, URI.create(sseUrl));
    EventSource eventSource = eventSourceBuilder.build();
    eventSource.start();

//    since the event source listens to SSE in a separate thread
    TimeUnit.MINUTES.sleep(1000);
  }
}
