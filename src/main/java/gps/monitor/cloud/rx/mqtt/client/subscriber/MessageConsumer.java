package gps.monitor.cloud.rx.mqtt.client.subscriber;

import gps.monitor.cloud.rx.mqtt.client.bus.Bus;

import java.util.function.Consumer;

/**
 * Interfaz que permite subscribirse a mensajes que emite un {@link Bus}
 * a traves de un {@link Consumer}
 *
 * @author daniel.carvajal
 */
public interface MessageConsumer<T> extends Consumer<T> {

}
