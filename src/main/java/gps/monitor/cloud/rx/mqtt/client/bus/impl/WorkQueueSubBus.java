package gps.monitor.cloud.rx.mqtt.client.bus.impl;

import gps.monitor.cloud.rx.mqtt.client.bus.Bus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.WorkQueueProcessor;

import java.util.List;
import java.util.function.Consumer;

/**
 * Representa la implementación de un {@link Bus} reactivo a traves de un {@link WorkQueueProcessor}
 * <p>
 * {@link WorkQueueSubBus} tiene el proposito de consumir mensajes de los topicos subcritos a traves del
 * {@link gps.monitor.cloud.rx.mqtt.client.integration.MqttGateway}.
 * <p>
 * Este bus trabaja parecido a una cola, 1 de los {@link Consumer} subscritos toma el mensaje actual del bus y lo procesa, este, no tiene garantia
 * que los mensajes se procesen en orden
 *
 * @author daniel.carvajal
 * @see <a href="https://projectreactor.io/docs/core/release/api/reactor/core/publisher/WorkQueueProcessor.html">Project Reactor WorkQueueProcessor</a>
 */
public class WorkQueueSubBus implements Bus {

    private WorkQueueProcessor<Object> processor = WorkQueueProcessor.create();

    private static final Logger logger = LoggerFactory.getLogger(WorkQueueSubBus.class);


    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(Consumer<Object> subscriber) {
        processor
            .publish()
                .autoConnect()
             .subscribe(subscriber, e -> {
                logger.error("[{}] Se ha producido un error en el flujo del subcriptor asincronico!!!", WorkQueueSubBus.class.getSimpleName());
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
                logger.error("[{}] Se ha producido un error en el flujo del subcriptor asincronico!!!", WorkQueueSubBus.class.getSimpleName());
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
