package gps.monitor.cloud.rx.mqtt.client.bus.impl;

import gps.monitor.cloud.rx.mqtt.client.bus.Bus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

/**
 * Representa la implementacion de un {@link Bus} a traves de un {@link DirectProcessor}
 * <p>
 * {@link DirectPubBus} tiene el proposito de mantener un {@link gps.monitor.cloud.rx.mqtt.client.publisher.MessagePublicator}
 * que publica mensajes enviados a un topico traves de {@link gps.monitor.cloud.rx.mqtt.client.integration.MqttGateway}
 *
 * @author daniel.carvajal
 * @see <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/DirectProcessor.html">Project Reactor DirectProcessor</a>
 */
public class DirectPubBus implements Bus {

    private static DirectProcessor<Object> processor = DirectProcessor.create();

    private static final Logger logger = LoggerFactory.getLogger(DirectPubBus.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(Consumer<Object> subscriber) {
        processor
            .publish()
                .autoConnect()
            .delayElements(Duration.ofMillis(500))
                .subscribe(subscriber, e -> {
                    logger.error("[{}] Se ha producido un error en el flujo del publicador!!!", DirectPubBus.class.getSimpleName());
                    logger.error(e.getMessage(), e);
                });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(List<Consumer<Object>> subscribers) {
        throw new UnsupportedOperationException("El [DirectPubBus] no soporta multiple consumidores!!!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> void handle(T message) {
        processor.onNext(message);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unSubscribe() {
        processor.dispose();
    }
}
