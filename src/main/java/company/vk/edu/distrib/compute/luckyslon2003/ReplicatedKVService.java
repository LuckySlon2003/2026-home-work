package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.ReplicatedService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.Executors;

// Реплицируемый KVService. С внешними клиентами общается по HTTP (как и раньше),
// а внутри проксирует чтения/записи на ноды-реплики по gRPC через ReplicaCoordinator.
public final class ReplicatedKVService implements ReplicatedService {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedKVService.class);

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";
    private static final String PATH_STATS = "/stats";
    private static final String STATS_PREFIX = "/stats/replica/";
    private static final String ACCESS_SUFFIX = "/access";
    private static final int DEFAULT_ACK = 1;

    private final int port;
    private final HttpServer server;
    private final ReplicaCoordinator coordinator;

    public ReplicatedKVService(int port, int replicas) throws IOException {
        this.port = port;
        this.coordinator = new ReplicaCoordinator(replicas);
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(Executors.newCachedThreadPool());
        this.server.createContext(PATH_STATUS, this::handleStatus);
        this.server.createContext(PATH_ENTITY, this::handleEntity);
        this.server.createContext(PATH_STATS, this::handleStats);
    }

    @Override
    public void start() {
        try {
            coordinator.start();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start replica nodes", e);
        }
        server.start();
        LOG.info("ReplicatedKVService started on http port {} with {} gRPC replicas",
                port, coordinator.numberOfReplicas());
    }

    @Override
    public void stop() {
        server.stop(0);
        coordinator.stop();
        LOG.info("ReplicatedKVService stopped");
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return coordinator.numberOfReplicas();
    }

    @Override
    public void disableReplica(int nodeId) {
        coordinator.setEnabled(nodeId, false);
    }

    @Override
    public void enableReplica(int nodeId) {
        coordinator.setEnabled(nodeId, true);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if ("GET".equals(exchange.getRequestMethod())) {
            sendEmpty(exchange, 200);
        } else {
            sendEmpty(exchange, 405);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String id = queryParam(exchange.getRequestURI(), "id");
        if (id == null || id.isEmpty()) {
            sendEmpty(exchange, 400);
            return;
        }
        int ack;
        try {
            ack = resolveAck(exchange.getRequestURI());
        } catch (IllegalArgumentException e) {
            sendEmpty(exchange, 400);
            return;
        }
        switch (exchange.getRequestMethod().toUpperCase(Locale.ROOT)) {
            case "GET" -> handleGet(exchange, id, ack);
            case "PUT" -> handlePut(exchange, id, ack);
            case "DELETE" -> handleDelete(exchange, id, ack);
            default -> sendEmpty(exchange, 405);
        }
    }

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        ReplicaCoordinator.ReadOutcome outcome = coordinator.read(id, ack);
        if (!outcome.quorum()) {
            sendEmpty(exchange, 500);
            return;
        }
        if (!outcome.found()) {
            sendEmpty(exchange, 404);
            return;
        }
        sendBytes(exchange, 200, outcome.value());
    }

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        byte[] body;
        try (InputStream in = exchange.getRequestBody()) {
            body = in.readAllBytes();
        }
        sendEmpty(exchange, coordinator.write(id, body, ack) ? 201 : 500);
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        sendEmpty(exchange, coordinator.remove(id, ack) ? 202 : 500);
    }

    private void handleStats(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendEmpty(exchange, 405);
            return;
        }
        String path = exchange.getRequestURI().getPath();
        if (!path.startsWith(STATS_PREFIX)) {
            sendEmpty(exchange, 404);
            return;
        }
        String rest = path.substring(STATS_PREFIX.length());
        boolean access = rest.endsWith(ACCESS_SUFFIX);
        if (access) {
            rest = rest.substring(0, rest.length() - ACCESS_SUFFIX.length());
        }
        int nodeId;
        try {
            nodeId = Integer.parseInt(rest);
        } catch (NumberFormatException e) {
            sendEmpty(exchange, 404);
            return;
        }
        if (nodeId < 0 || nodeId >= coordinator.numberOfReplicas()) {
            sendEmpty(exchange, 404);
            return;
        }
        sendBytes(exchange, 200, statsJson(nodeId, access).getBytes(StandardCharsets.UTF_8));
    }

    private String statsJson(int nodeId, boolean access) {
        if (access) {
            return "{\"reads\":" + coordinator.reads(nodeId)
                    + ",\"writes\":" + coordinator.writes(nodeId) + "}";
        }
        return "{\"storedKeys\":" + coordinator.storedKeys(nodeId) + "}";
    }

    private int resolveAck(URI uri) {
        String raw = queryParam(uri, "ack");
        int ack = DEFAULT_ACK;
        if (raw != null) {
            ack = Integer.parseInt(raw);
        }
        if (ack < 1 || ack > coordinator.numberOfReplicas()) {
            throw new IllegalArgumentException("ack out of range");
        }
        return ack;
    }

    private static void sendEmpty(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
        exchange.close();
    }

    private static void sendBytes(HttpExchange exchange, int status, byte[] body) throws IOException {
        if (body.length == 0) {
            sendEmpty(exchange, status);
            return;
        }
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
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
