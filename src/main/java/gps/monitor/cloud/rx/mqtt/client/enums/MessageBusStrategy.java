package gps.monitor.cloud.rx.mqtt.client.enums;

import gps.monitor.cloud.rx.mqtt.client.bus.Bus;

/**
 * Mantiene las estrategias para la los {@link Bus},
 * estas son las siguientes:
 * <p>
 * - SECUENCE_STRATEGY : Estrategia que se base en stream secuenciales
 * <p>
 * - ASYNCHRONOUS_STRATEGY : Estrategia que se base en stream secuenciales con multiples subcriptores
 * <p>
 * - ASYNCHRONOUS_PARALLEL_STRATEGY: Estrategia que se base en stream con multiple subcritores en paralelo
 *
 * @author daniel.carvajal
 *
 */
public enum MessageBusStrategy {
    SECUENCE_STRATEGY("SECUENCE_STRATEGY", "Estrategia que se base en stream secuenciales"),
    ASYNCHRONOUS_STRATEGY("ASYNCHRONOUS_STRATEGY", "Estrategia que se base en stream con multiples subcriptores"),
    ASYNCHRONOUS_PARALLEL_STRATEGY("ASYNCHRONOUS_PARALLEL_STRATEGY", "Estrategia que se base en stream con multiple subcritores en paralelo");

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
