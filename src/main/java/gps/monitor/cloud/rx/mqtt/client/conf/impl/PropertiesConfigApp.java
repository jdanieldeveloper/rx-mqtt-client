package gps.monitor.cloud.rx.mqtt.client.conf.impl;


import gps.monitor.cloud.rx.mqtt.client.conf.Configuration;
import gps.monitor.cloud.rx.mqtt.client.util.UtilProperties;

/**
 * Representa la configuracion para los properties de {@link java.util.logging.Logger}
 *
 * @author daniel.carvajal
 */

public class PropertiesConfigApp implements Configuration {

    @Override
    public void configure() {
        configureLogger(); // configure logger
    }

    private void configureLogger() {
        UtilProperties.configure("/jsr47min.properties");
    }

    private void configureProperties() {
        // TODO ver si esta clase se debe utilizar con algo
    }

    @Override
    public void destroy() {
        //TODO ver si esta clase se debe utilizar con algo
    }
}
