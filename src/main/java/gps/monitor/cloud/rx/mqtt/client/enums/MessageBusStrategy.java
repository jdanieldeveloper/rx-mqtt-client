package gps.monitor.cloud.rx.mqtt.client.enums;

import gps.monitor.cloud.rx.mqtt.client.bus.Bus;

/**
 * Mantiene las estrategias para la los {@link Bus},
 * estas son las siguientes:
 * <p>
 *  - NATIVE_STRATEGY : Estrategia que se base en stream secuenciales sin backpressure
 * <p>
 * - SECUENCE_STRATEGY : Estrategia que se base en stream secuenciales con backpressure
 * <p>
 * - ASYNCHRONOUS_STRATEGY : Estrategia que se base en stream secuenciales con multiples subcriptores con backpressure
 * <p>
 * - ASYNCHRONOUS_PARALLEL_STRATEGY: Estrategia que se base en stream con multiple subcritores en paralelo con backpressure
 *
 * @author daniel.carvajal
 *
 */
public enum MessageBusStrategy {
    NATIVE_STRATEGY("NATIVE_STRATEGY", "Estrategia que se base en stream secuenciales sin backpressure"),
    SECUENCE_STRATEGY("SECUENCE_STRATEGY", "Estrategia que se base en stream secuenciales con backpressure"),
    ASYNCHRONOUS_STRATEGY("ASYNCHRONOUS_STRATEGY", "Estrategia que se base en stream con multiples subcriptores con backpressure"),
    ASYNCHRONOUS_PARALLEL_STRATEGY("ASYNCHRONOUS_PARALLEL_STRATEGY", "Estrategia que se base en stream con multiple subcritores en paralelo con backpressure");

    private final String id;
    private final String decription;

    private MessageBusStrategy(String id, String decription) {
        this.id = id;
        this.decription = decription;
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return decription;
    }
}
