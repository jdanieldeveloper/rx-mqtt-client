package gps.monitor.cloud.rx.mqtt.client.bus;

import java.util.List;
import java.util.function.Consumer;

/**
 * Representa un MessageBus
 *
 * @author daniel.carvajal
 * @see <a href="https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageBus.html">EIP MessageBus</a>
 */
public interface Bus {

    /**
     * Subscribe un {@link Consumer} al Bus
     *
     * @param subscriber
     */
    void subscribe(Consumer<Object> subscriber);

    /**
     * Subscribe una {@link List} de {@link Consumer} al Bus
     *
     * @param subscribers
     */
    void subscribe(List<Consumer<Object>> subscribers);

    /**
     * Maneja un mensaje y lo envia a los {@link Consumer} subscritos para su procesamiento
     *
     * @param message
     * @param <M>
     */
    <M> void handle(M message);

    /**
     * Des-Subscribe los {@link Consumer} subscritos al bus
     */
    void unSubscribe();

}
