package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.Dao;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class LuckySlon2003AuditableKVService implements AuditableKVService {

    private static final Logger LOG = LoggerFactory.getLogger(LuckySlon2003AuditableKVService.class);
    private static final String AUDIT_TOPIC = "audit";

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final AtomicBoolean async = new AtomicBoolean(false);
    private KafkaProducer<String, String> producer;

    public LuckySlon2003AuditableKVService(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(Executors.newCachedThreadPool());
        this.server.createContext("/v0/status", this::handleStatus);
        this.server.createContext("/v0/entity", this::handleEntity);
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void setAsync(boolean enabled) {
        this.async.set(enabled);
    }

    @Override
    public void start() {
        server.start();
        if (LOG.isInfoEnabled()) {
            LOG.info("LuckySlon2003AuditableKVService started on port {}", server.getAddress().getPort());
        }
    }

    @Override
    public void stop() {
        server.stop(1);
        if (producer != null) {
            producer.close();
        }
        try {
            dao.close();
        } catch (IOException e) {
            LOG.error("Error closing DAO", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!Objects.equals(exchange.getRequestMethod(), "GET")) {
            sendEmpty(exchange, 405);
            return;
        }
        sendEmpty(exchange, 200);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String id = queryParam(exchange.getRequestURI(), "id");
        if (id == null || id.isEmpty()) {
            sendEmpty(exchange, 400);
            return;
        }

        String method = exchange.getRequestMethod().toUpperCase(Locale.ROOT);
        long timestamp = System.currentTimeMillis();

        sendAuditEvent(method, id, timestamp);

        switch (method) {
            case "GET" -> handleGet(exchange, id);
            case "PUT" -> handlePut(exchange, id);
            case "DELETE" -> handleDelete(exchange, id);
            default -> sendEmpty(exchange, 405);
        }
    }

    private void sendAuditEvent(String method, String id, long timestamp) {
        if (producer == null) {
            return;
        }
        String value = method + "|" + id + "|" + timestamp;
        ProducerRecord<String, String> record = new ProducerRecord<>(AUDIT_TOPIC, id, value);
        if (async.get()) {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Async audit send failed", exception);
                }
            });
        } else {
            try {
                producer.send(record).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Audit send interrupted", e);
            } catch (ExecutionException e) {
                LOG.error("Audit send failed", e);
            }
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] data = dao.get(id);
            exchange.sendResponseHeaders(200, data.length);
            try (var out = exchange.getResponseBody()) {
                out.write(data);
            }
        } catch (NoSuchElementException e) {
            sendEmpty(exchange, 404);
        } catch (IllegalArgumentException e) {
            sendEmpty(exchange, 400);
        } catch (IOException e) {
            LOG.error("GET error for id={}", id, e);
            sendEmpty(exchange, 500);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        try (InputStream body = exchange.getRequestBody()) {
            byte[] data = body.readAllBytes();
            dao.upsert(id, data);
            sendEmpty(exchange, 201);
        } catch (IllegalArgumentException e) {
            sendEmpty(exchange, 400);
        } catch (IOException e) {
            LOG.error("PUT error for id={}", id, e);
            sendEmpty(exchange, 500);
        }
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        try {
            dao.delete(id);
            sendEmpty(exchange, 202);
        } catch (IllegalArgumentException e) {
            sendEmpty(exchange, 400);
        } catch (IOException e) {
            LOG.error("DELETE error for id={}", id, e);
            sendEmpty(exchange, 500);
        }
    }

    private static void sendEmpty(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
        exchange.close();
    }

    private static String queryParam(URI uri, String name) {
        String query = uri.getRawQuery();
        if (query == null) {
            return null;
        }
        String prefix = name + "=";
        for (String pair : query.split("&")) {
            if (pair.startsWith(prefix)) {
                return pair.substring(prefix.length());
            }
        }
        return null;
    }
}
