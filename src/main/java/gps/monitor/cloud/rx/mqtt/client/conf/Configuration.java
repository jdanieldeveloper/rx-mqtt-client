package gps.monitor.cloud.rx.mqtt.client.conf;

/**
 * Representa una configuracion para algun servicio
 *
 * @author daniel.carvajal
 */
public interface Configuration {

    /**
     * Se encarga de configurar los parametros para una configuracion especifica
     */
    void configure();

    /**
     * * Se encarga de destruir los recursos para una configuracion especifica
     */
    void destroy();
}
