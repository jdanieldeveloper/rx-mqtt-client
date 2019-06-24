package gps.monitor.cloud.rx.mqtt.client.util;


import java.io.IOException;
import java.io.InputStream;

import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Clase de utilidad para obtener los properties desde el classpath para el {@link java.util.logging.Logging}
 *
 * @author daniel.carvajal
 */
public class UtilProperties {

    private UtilProperties() {
    }

    /**
     * Obtiene desde el classpath el archivo .properties especificado como parametros
     *
     * @param configFileName
     */
    public static void configure(String configFileName) {
        InputStream istream = null;
        try {
            istream = UtilProperties.class.getResourceAsStream(configFileName);
            if (Objects.nonNull(istream)) {
                LogManager.getLogManager().readConfiguration(istream);
            } else {
                Logger.getGlobal().log(Level.SEVERE, String.format("[%s] No se pudo leer el archivo de configuracion [%s]", UtilProperties.class.getSimpleName(), configFileName));
                Logger.getGlobal().log(Level.SEVERE, String.format("[%s] Ignorando el archivo de configuracion [%s]", UtilProperties.class.getSimpleName(), configFileName));
            }

        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE, String.format("[%s] No se pudo leer el archivo de configuracion [%s]", UtilProperties.class.getSimpleName(), configFileName), e);
            Logger.getGlobal().log(Level.SEVERE, String.format("[%s] Ignorando el archivo de configuracion [%s]", UtilProperties.class.getSimpleName(), configFileName), e);
            Logger.getGlobal().log(Level.SEVERE, e.getMessage(), e);
        } finally {
            if (istream != null) {
                try {
                    istream.close();
                } catch (IOException e) {
                    Logger.getGlobal().log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }
    }
}
