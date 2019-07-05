package gps.monitor.cloud.rx.mqtt.client.bus.impl;

import gps.monitor.cloud.rx.mqtt.client.bus.Bus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.*;

import java.util.List;
import java.util.function.Consumer;

/**
 * Representa la implementaci�n de un {@link Bus} reactivo a traves de un {@link EmitterProcessor}
 * <p>
 * {@link EmitterSubBus} tiene el proposito de consumir mensajes de los topicos subcritos a traves del
 * {@link gps.monitor.cloud.rx.mqtt.client.integration.MqttGateway}.
 * <p>
 * Este bus trabaja parecido a una lista, donde los {@link Consumer} subscritos toma el mensaje actual del bus
 * y lo procesa garantizando el orden de procesamiento
 *
 * @author daniel.carvajal
 * @see <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/EmitterProcessor.html">Project Reactor EmitterProcessor</a>
 */
public class EmitterSubBus implements Bus {

    private static EmitterProcessor<Object> processor = EmitterProcessor.create();

    private static final Logger logger = LoggerFactory.getLogger(EmitterSubBus.class.getCanonicalName());

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(Consumer<Object> subscriber) {
        processor
            .publish()
                .autoConnect()
            .subscribe(subscriber, e -> {
                logger.error("[{}] Se ha producido un error en el flujo del subcriptor!!!", EmitterSubBus.class.getSimpleName());
                logger.error(e.getMessage(), e);
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(List<Consumer<Object>> subscribers) {
        processor
            .publish()
                .autoConnect();
        for (Consumer<Object> subscriber : subscribers) {
            processor.subscribe(subscriber, e -> {
                logger.error("[{}] Se ha producido un error en el flujo del subcriptor!!!", EmitterSubBus.class.getSimpleName());
                logger.error(e.getMessage(), e);
            });
        }
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
