package company.vk.edu.distrib.compute.luckyslon2003;

// Реплика не подтвердила операцию (выключена или gRPC-вызов завершился ошибкой).
// Такая реплика не учитывается в кворуме ack.
final class ReplicaUnavailableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    ReplicaUnavailableException(Throwable cause) {
        super(cause);
    }
}
