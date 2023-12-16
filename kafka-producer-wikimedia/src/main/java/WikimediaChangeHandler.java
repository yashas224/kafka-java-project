import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangeHandler.class.getName());
  private KafkaProducer<String, String> kafkaProducer;
  private String topic;

  public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
    this.kafkaProducer = kafkaProducer;
    this.topic = topic;
  }

  public WikimediaChangeHandler() {
  }

  @Override
  public void onOpen() {
    LOGGER.info("Event Source opened !!");
  }

  @Override
  public void onClosed() {
    LOGGER.info("Event Source closed and closing kafka producer!!");
    kafkaProducer.close();
  }

  @Override
  public void onMessage(String event, MessageEvent messageEvent) throws Exception {
    String eventData = messageEvent.getData();
    ProducerRecord producerRecord = new ProducerRecord(topic, eventData);
    kafkaProducer.send(producerRecord);
    LOGGER.info("produced Event to kafka topic {}", eventData);
  }

  @Override
  public void onComment(String comment) throws Exception {
  }

  @Override
  public void onError(Throwable t) {
    kafkaProducer.close();
    LOGGER.error("Error !!!", t);
  }
}
