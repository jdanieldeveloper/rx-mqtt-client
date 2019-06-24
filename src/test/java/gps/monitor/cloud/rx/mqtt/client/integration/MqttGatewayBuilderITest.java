package gps.monitor.cloud.rx.mqtt.client.integration;

import gps.monitor.cloud.rx.mqtt.client.conf.impl.MqttServerConfigApp;
import gps.monitor.cloud.rx.mqtt.client.conf.impl.PropertiesConfigApp;
import junit.framework.TestCase;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Test;

/**
 * Test de integracion que permite ver las distintas posibilidades de construccion
 * del la clase {@link MqttGatewayBuilder}
 *
 * @author daniel.carvajal
 */
public class MqttGatewayBuilderITest extends TestCase {

    private MqttServerConfigApp mqttServerConfigApp;

    /**
     * Levanta un server mqtt embebido para purebas locales
     *
     * @throws Exception
     */
    @Override
    protected void setUp() throws Exception {
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();
        //
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa
     *
     * @throws MqttException
     */
    @Test
    public void testBuildBasicMqttAysncClientOkWithConnection() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                }).create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);
        //
        mqttGateway.detach();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker que NO es exitosa
     *
     * @throws MqttException
     */
    @Test
    public void testBuildBasicMqttAysncClientOkWithOutConnection() throws MqttException {
        mqttServerConfigApp.destroy(); // stop broker
        //
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                }).create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), false);
        //
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt
     *
     * @throws MqttException
     */
    @Test
    public void testBuildBasicMqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = true;
                }).create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.getMqttAsyncClient().isConnected(), true);
        //

        mqttGateway.detach();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker No exitosa mostrando
     * el debug del cliente mqtt
     *
     * @throws MqttException
     */
    @Test
    public void testBuildBasicMqttAysncClientOkWithOutConnectionDebug() throws MqttException {
        mqttServerConfigApp.destroy(); // stop broker
        //
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                    $.debug = true;
                }).create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), false);
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt con opciones de conexion por defecto     *
     *
     * Las opciones por defectos son las mismas que provee las del cliente paho
     *
     * @throws MqttException
     */
    @Test
    public void testBuildConnectionOptions0MqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                    $.debug = true;
                }).create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);

        // default options
        assertEquals(mqttGateway.getConnectOptions().isCleanSession(), true);
        assertEquals(mqttGateway.getConnectOptions().getKeepAliveInterval(), 60);
        assertEquals(mqttGateway.getConnectOptions().isAutomaticReconnect(), false);
        assertEquals(mqttGateway.getConnectOptions().getConnectionTimeout(), 30);
        assertEquals(mqttGateway.getConnectOptions().getMaxInflight(), 10);
        //
        mqttGateway.detach();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt con algunas opciones de conexion personalizadas que son:
     * - cleanSession = false
     *
     * y otras quedan por defecto.
     *
     * Las opciones por defectos son las mismas que provee las del cliente paho
     *
     * @throws MqttException
     */
    @Test
    public void testBuildConnectionOptions1MqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                    $.debug = true;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = false;
                            }).createConnectOptions();
                })
                .create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);
        // option
        assertEquals(mqttGateway.getConnectOptions().isCleanSession(), false);
        // default options
        assertEquals(mqttGateway.getConnectOptions().getKeepAliveInterval(), 60);
        assertEquals(mqttGateway.getConnectOptions().isAutomaticReconnect(), false);
        assertEquals(mqttGateway.getConnectOptions().getConnectionTimeout(), 30);
        assertEquals(mqttGateway.getConnectOptions().getMaxInflight(), 10);
        //
        mqttGateway.detach();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt con algunas opciones de conexion personalizadas que son:
     * - cleanSession = false
     * - keepAliveInterval = 120
     *
     * y otras quedan por defecto.
     *
     * Las opciones por defectos son las mismas que provee las del cliente paho
     *
     * @throws MqttException
     */
    @Test
    public void testBuildConnectionOptions2MqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                    $.debug = true;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = false;
                                $cOptions.keepAliveInterval = 120;
                            }).createConnectOptions();
                })
                .create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);
        // option
        assertEquals(mqttGateway.getConnectOptions().isCleanSession(), false);
        assertEquals(mqttGateway.getConnectOptions().getKeepAliveInterval(), 120);
        // default options
        assertEquals(mqttGateway.getConnectOptions().isAutomaticReconnect(), false);
        assertEquals(mqttGateway.getConnectOptions().getConnectionTimeout(), 30);
        assertEquals(mqttGateway.getConnectOptions().getMaxInflight(), 10);
        //
        mqttGateway.detach();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt con algunas opciones de conexion personalizadas que son:
     * - cleanSession = false
     * - keepAliveInterval = 120
     * - automaticReconnect = true
     *
     * y otras quedan por defecto.
     *
     * Las opciones por defectos son las mismas que provee las del cliente paho
     *
     * @throws MqttException
     */
    @Test
    public void testBuildConnectionOptions3MqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                    $.debug = true;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = false;
                                $cOptions.keepAliveInterval = 120;
                                $cOptions.automaticReconnect = true;
                            }).createConnectOptions();
                })
                .create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);
        // option
        assertEquals(mqttGateway.getConnectOptions().isCleanSession(), false);
        assertEquals(mqttGateway.getConnectOptions().getKeepAliveInterval(), 120);
        assertEquals(mqttGateway.getConnectOptions().isAutomaticReconnect(), true);
        // default options
        assertEquals(mqttGateway.getConnectOptions().getConnectionTimeout(), 30);
        assertEquals(mqttGateway.getConnectOptions().getMaxInflight(), 10);
        //
        mqttGateway.detach();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt con algunas opciones de conexion personalizadas que son:
     * - cleanSession = false
     * - keepAliveInterval = 120
     * - automaticReconnect = true
     * - connectionTimeout = 60
     *
     * y otras quedan por defecto.
     *
     * Las opciones por defectos son las mismas que provee las del cliente paho
     *
     * @throws MqttException
     */
    @Test
    public void testBuildConnectionOptions4MqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                    $.debug = true;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = false;
                                $cOptions.keepAliveInterval = 120;
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 60;
                            }).createConnectOptions();
                })
                .create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);
        // option
        assertEquals(mqttGateway.getConnectOptions().isCleanSession(), false);
        assertEquals(mqttGateway.getConnectOptions().getKeepAliveInterval(), 120);
        assertEquals(mqttGateway.getConnectOptions().isAutomaticReconnect(), true);
        assertEquals(mqttGateway.getConnectOptions().getConnectionTimeout(), 60);
        // default options
        assertEquals(mqttGateway.getConnectOptions().getMaxInflight(), 10);
        //
        mqttGateway.detach();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt con algunas opciones de conexion personalizadas que son:
     * - cleanSession = false
     * - keepAliveInterval = 120
     * - automaticReconnect = true
     * - connectionTimeout = 60
     * - maxInflight = 20
     *
     * Las opciones por defectos son las mismas que provee las del cliente paho
     *
     * @throws MqttException
     */
    @Test
    public void testBuildConnectionOptions5MqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                    $.debug = true;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = false;
                                $cOptions.keepAliveInterval = 120;
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 60;
                                $cOptions.maxInflight = 20;
                            }).createConnectOptions();
                })
                .create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);
        // option
        assertEquals(mqttGateway.getConnectOptions().isCleanSession(), false);
        assertEquals(mqttGateway.getConnectOptions().getKeepAliveInterval(), 120);
        assertEquals(mqttGateway.getConnectOptions().isAutomaticReconnect(), true);
        assertEquals(mqttGateway.getConnectOptions().getConnectionTimeout(), 60);
        assertEquals(mqttGateway.getConnectOptions().getMaxInflight(), 20);
        // without default options

        //
        mqttGateway.detach();
    }



    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt con las opciones del buffer por defecto.
     *
     * Las opciones por defectos son las mismas que provee las del cliente paho
     *
     * @throws MqttException
     */
    @Test
    public void testBuildBufferOptions0MqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = true;
                })
                .create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);

        // default options
        assertEquals(mqttGateway.getBufferOptions().isBufferEnabled(), false);
        assertEquals(mqttGateway.getBufferOptions().isPersistBuffer(), false);
        assertEquals(mqttGateway.getBufferOptions().getBufferSize(),  5000);
        assertEquals(mqttGateway.getBufferOptions().isDeleteOldestMessages(), false);
        //
        mqttGateway.detach();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt con algunas opciones del buffer personalizadas que son:
     * - bufferEnabled = true
     *
     * Las opciones por defectos son las mismas que provee las del cliente paho
     *
     * @throws MqttException
     */
    @Test
    public void testBuildBufferOptions1MqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = true;
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = true;
                            }).createBufferOptions();
                })
                .create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);
        // option
        assertEquals(mqttGateway.getBufferOptions().isBufferEnabled(), true);
        // default options
        assertEquals(mqttGateway.getBufferOptions().isPersistBuffer(), false);
        assertEquals(mqttGateway.getBufferOptions().getBufferSize(),  5000);
        assertEquals(mqttGateway.getBufferOptions().isDeleteOldestMessages(), false);
        //
        mqttGateway.detach();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt con algunas opciones del buffer personalizadas que son:
     * - bufferEnabled = true
     * - persistBuffer = true
     *
     * Las opciones por defectos son las mismas que provee las del cliente paho
     *
     * @throws MqttException
     */
    @Test
    public void testBuildBufferOptions2MqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = true;
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = true;
                                $bOptions.persistBuffer = true;
                            }).createBufferOptions();
                })
                .create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);
        // option
        assertEquals(mqttGateway.getBufferOptions().isBufferEnabled(), true);
        assertEquals(mqttGateway.getBufferOptions().isPersistBuffer(), true);
        // default options
        assertEquals(mqttGateway.getBufferOptions().getBufferSize(),  5000);
        assertEquals(mqttGateway.getBufferOptions().isDeleteOldestMessages(), false);
        //
        mqttGateway.detach();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt con algunas opciones del buffer personalizadas que son:
     * - bufferEnabled = true
     * - persistBuffer = true
     * - bufferSize = 10000
     *
     * Las opciones por defectos son las mismas que provee las del cliente paho
     *
     * @throws MqttException
     */
    @Test
    public void testBuildBufferOptions3MqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = true;
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = true;
                                $bOptions.persistBuffer = true;
                                $bOptions.bufferSize = 10000;

                            }).createBufferOptions();
                })
                .create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);
        // option
        assertEquals(mqttGateway.getBufferOptions().isBufferEnabled(), true);
        assertEquals(mqttGateway.getBufferOptions().isPersistBuffer(), true);
        assertEquals(mqttGateway.getBufferOptions().getBufferSize(),  10000);
        // default options
        assertEquals(mqttGateway.getBufferOptions().isDeleteOldestMessages(), false);
        //
        mqttGateway.detach();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa mostrando
     * el debug del cliente mqtt con algunas opciones del buffer personalizadas que son:
     * - bufferEnabled = true
     * - persistBuffer = true
     * - bufferSize = 10000
     * - deleteOldestMessages = false
     *
     * Las opciones por defectos son las mismas que provee las del cliente paho
     *
     * @throws MqttException
     */
    @Test
    public void testBuildBufferOptions4MqttAysncClientOkWithConnectionDebug() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = true;
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = true;
                                $bOptions.persistBuffer = true;
                                $bOptions.bufferSize = 10000;
                                $bOptions.deleteOldestMessages = false;
                            }).createBufferOptions();
                })
                .create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);
        // option
        assertEquals(mqttGateway.getBufferOptions().isBufferEnabled(), true);
        assertEquals(mqttGateway.getBufferOptions().isPersistBuffer(), true);
        assertEquals(mqttGateway.getBufferOptions().getBufferSize(),  10000);
        assertEquals(mqttGateway.getBufferOptions().isDeleteOldestMessages(), false);
        // without default options

        //
        mqttGateway.detach();
    }


    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa del cliente mqtt
     * con TODAS las opciones de conexion  y del buffer disponibles
     *
     * @throws MqttException
     */
    @Test
    public void testBuildWithOptionsMqttAysncClientOkWithConnection() throws MqttException {
        MqttGateway mqttGateway = new MqttGatewayBuilder()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                            }).createConnectOptions();
                })
                .with($ -> {
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = true;
                                $bOptions.persistBuffer = true;
                                $bOptions.bufferSize = 10000;
                                $bOptions.deleteOldestMessages = true;
                            }).createBufferOptions();
                })
                .create();
        //
        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1883");
        assertEquals(mqttGateway.isConnected(), true);
        // connect option
        assertEquals(mqttGateway.getConnectOptions().isCleanSession(), true);
        assertEquals(mqttGateway.getConnectOptions().getKeepAliveInterval(), 60);
        assertEquals(mqttGateway.getConnectOptions().isAutomaticReconnect(), true);
        assertEquals(mqttGateway.getConnectOptions().getConnectionTimeout(), 30);
        assertEquals(mqttGateway.getConnectOptions().getMaxInflight(), 1000);
        //
        // buffer option
        assertEquals(mqttGateway.getBufferOptions().isBufferEnabled(), true);
        assertEquals(mqttGateway.getBufferOptions().isPersistBuffer(), true);
        assertEquals(mqttGateway.getBufferOptions().getBufferSize(),  10000);
        assertEquals(mqttGateway.getBufferOptions().isDeleteOldestMessages(), true);
        //

        mqttGateway.detach();
    }

    @Override
    protected void tearDown() throws Exception {
        mqttServerConfigApp.destroy();
    }
}
