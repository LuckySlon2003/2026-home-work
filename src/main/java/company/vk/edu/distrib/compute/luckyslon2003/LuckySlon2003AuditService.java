package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class LuckySlon2003AuditService implements AuditService {

    private static final Logger LOG = LoggerFactory.getLogger(LuckySlon2003AuditService.class);
    private static final String AUDIT_TOPIC = "audit";

    private final String bootstrapServers;
    private final String consumerGroupId;
    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private KafkaConsumer<String, String> consumer;
    private Thread pollingThread;

    public LuckySlon2003AuditService(String bootstrapServers, String consumerGroupId) {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
    }

    @Override
    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(AUDIT_TOPIC));

        running.set(true);
        pollingThread = new Thread(this::pollLoop, "audit-consumer-" + consumerGroupId);
        pollingThread.setDaemon(true);
        pollingThread.start();
    }

    private void pollLoop() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    AuditEvent event = deserialize(record.value());
                    if (event != null) {
                        events.add(event);
                    }
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                LOG.error("Unexpected wakeup", e);
            }
        }
    }

    @Override
    public void stop() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
        if (pollingThread != null) {
            try {
                pollingThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return new ArrayList<>(events);
    }

    private static AuditEvent deserialize(String value) {
        if (value == null) {
            return null;
        }
        String[] parts = value.split("\\|", 3);
        if (parts.length < 3) {
            LOG.warn("Invalid audit event format: {}", value);
            return null;
        }
        try {
            return new AuditEvent(parts[0], parts[1], Long.parseLong(parts[2]));
        } catch (NumberFormatException e) {
            LOG.warn("Invalid timestamp in audit event: {}", value, e);
            return null;
        }
    }
}
