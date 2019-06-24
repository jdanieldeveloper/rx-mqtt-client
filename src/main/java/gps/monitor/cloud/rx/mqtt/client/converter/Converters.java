package gps.monitor.cloud.rx.mqtt.client.converter;

import java.nio.charset.StandardCharsets;

/**
 * Mantiene las conversiones de tipo de la libreria
 *
 * @author daniel.carvajal
 */
public class Converters {


    /**
     * Convierte de un array de bytes a un string
     *
     * @param bytes
     * @return nuevo string
     */
    public static String bytesToString(byte[] bytes){
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
