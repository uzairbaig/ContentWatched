package co.uk.sky.content;

import static java.time.LocalDateTime.now;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EventProducerTest {

    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();
    ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

    private ObjectMapper objectMapper = new ObjectMapper();
    final String sessionId = randomUUID().toString();
    private long startTimestamp;
    private String userId;
    private String contentId;


    @Before
    public void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        final EventProducer eventProducer = new EventProducer();
        final Topology topology = eventProducer.createTopology();

        testDriver = new TopologyTestDriver(topology, props);
    }

    @Test
    public void shouldPublishContentWatchedEvent() {
        userId = randomUUID().toString();
        contentId = randomUUID().toString();
        final LocalDateTime now = now().minusMinutes(15);
        final LocalDateTime endTime = now();
        startTimestamp = getEpocTime(now);
        final long endTimestamp = getEpocTime(endTime);

        pushNewRecord(EventType.START, startTimestamp);
        pushNewRecord(EventType.STOP, endTimestamp);
        final long minutes = Duration.between(now, endTime).toMinutes();

        final ProducerRecord<String, String> stringStringProducerRecord = readOutput();
        OutputVerifier.compareKeyValue(stringStringProducerRecord, sessionId, getContentWatchedAsJson(startTimestamp, minutes));

    }

    @Test
    public void shouldPublishContentWatchedEventWhenEventComeInDifferentOrder() {
        userId = randomUUID().toString();
        contentId = randomUUID().toString();
        final LocalDateTime now = now().minusMinutes(20);
        final LocalDateTime endTime = now();
        startTimestamp = getEpocTime(now);
        final long endTimestamp = getEpocTime(endTime);

        pushNewRecord(EventType.STOP, endTimestamp);
        pushNewRecord(EventType.START, startTimestamp);
        final long minutes = Duration.between(now, endTime).toMinutes();

        final ProducerRecord<String, String> stringStringProducerRecord = readOutput();
        OutputVerifier.compareKeyValue(stringStringProducerRecord, sessionId, getContentWatchedAsJson(startTimestamp, minutes));

    }

    @Test
    public void shouldNoPublishContentWatchedEventWhenDurationIsLessThen10m() {
        userId = randomUUID().toString();
        contentId = randomUUID().toString();
        final LocalDateTime now = now().minusMinutes(5);
        final LocalDateTime endTime = now();
        startTimestamp = getEpocTime(now);
        final long endTimestamp = getEpocTime(endTime);

        pushNewRecord(EventType.START, startTimestamp);
        pushNewRecord(EventType.STOP, endTimestamp);

        final ProducerRecord<String, String> stringStringProducerRecord = readOutput();
        Assert.assertNull(stringStringProducerRecord);
    }

    @Test
    public void shouldNoPublishContentWatchedEventWhenDurationIsMoreThen8Hours() {
        userId = randomUUID().toString();
        contentId = randomUUID().toString();
        final LocalDateTime now = now().minusHours(9);
        final LocalDateTime endTime = now();
        startTimestamp = getEpocTime(now);
        final long endTimestamp = getEpocTime(endTime);

        pushNewRecord(EventType.START, startTimestamp);
        pushNewRecord(EventType.STOP, endTimestamp);

        final ProducerRecord<String, String> stringStringProducerRecord = readOutput();
        Assert.assertNull(stringStringProducerRecord);
    }

    public void pushNewRecord(final EventType eventType, final long timestamp) {
        testDriver.pipeInput(factory.create("playout-topic", null, getPlayoutEventAsJson(eventType, timestamp)));
    }

    public ProducerRecord<String, String> readOutput() {
        return testDriver.readOutput("content-watched-topic", new StringDeserializer(), new StringDeserializer());
    }

    private String getPlayoutEventAsJson(final EventType eventType, final long timestamp) {
        PlayOutEvent playoutEvent = null;

        if (EventType.START == eventType) {
            playoutEvent = new PlayOutEvent(timestamp, sessionId, EventType.START, userId, contentId);
        } else {
            playoutEvent = new PlayOutEvent(timestamp, sessionId, EventType.STOP);
        }

        try {
            return objectMapper.writeValueAsString(playoutEvent);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getContentWatchedAsJson(final long timestamp, final long duration) {
        ContentWatched cw = new ContentWatched(timestamp, userId, contentId, MINUTES.toSeconds(duration));
        try {
            return objectMapper.writeValueAsString(cw);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Long getEpocTime(final LocalDateTime date) {
        ZoneId zoneId = ZoneId.systemDefault();
        return date.atZone(zoneId).toEpochSecond();
    }
}
