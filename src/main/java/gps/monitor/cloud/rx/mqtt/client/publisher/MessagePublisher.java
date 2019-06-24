package gps.monitor.cloud.rx.mqtt.client.publisher;

import gps.monitor.cloud.rx.mqtt.client.integration.MqttGateway;

import java.util.function.Consumer;

/**
 * Interfaz que permite publicar a mensajes a un {@link gps.monitor.cloud.rx.mqtt.client.bus.Bus}
 * a traves de un {@link Consumer}
 *
 * @author daniel.carvajal
 */
public interface MessagePublisher<T> extends Consumer<T> {

    MqttGateway getMqttGateway();

    void setMqttGateway(MqttGateway mqttGateway);
}
