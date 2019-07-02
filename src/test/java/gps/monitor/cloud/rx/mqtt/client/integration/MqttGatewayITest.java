package gps.monitor.cloud.rx.mqtt.client.integration;

import gps.monitor.cloud.rx.mqtt.client.conf.impl.MqttServerConfigApp;
import gps.monitor.cloud.rx.mqtt.client.conf.impl.PropertiesConfigApp;
import gps.monitor.cloud.rx.mqtt.client.enums.MessageBusStrategy;
import gps.monitor.cloud.rx.mqtt.client.listener.DefaultMqttListener;
import gps.monitor.cloud.rx.mqtt.client.subscriber.TestSubcriberConsumer;
import junit.framework.TestCase;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Test;
import org.netcrusher.core.reactor.NioReactor;
import org.netcrusher.tcp.TcpCrusher;
import org.netcrusher.tcp.TcpCrusherBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by daniel.carvajal on 20-06-2018.
 */
public class MqttGatewayITest extends TestCase {

    private MqttServerConfigApp mqttServerConfigApp;

    @Override
    protected void setUp() throws Exception {

    }
     /**
     * Construye un cliente basico y realiza una conexion al broker exitosa desde el cliente mqtt
     * con algunas opciones de conexion  y del buffer disponibles agregando un subcriptor a un topico mock
     *
     * @throws MqttException
     */
    @Test
    public void testSubscriberWithConnectionOk() throws MqttException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();
        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = true;
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
                                $bOptions.bufferEnabled = false;
                            }).createBufferOptions();
                })
                .with($ -> {
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock";
                                $sOptions.qos = 2;
                            }).createSubcriberOptions();
                })
                .create()
         .subscribeWithOptions();
        //
        mqttServerConfigApp.destroy();
    }

    @Test
    public void test2SubscriberWithConnectionOk() throws MqttException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();
        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    $.host = "127.0.0.1";
                    $.port = 1883;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = true;
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
                                $bOptions.bufferEnabled = false;
                            }).createBufferOptions();
                })
                .with($ -> {
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock1";
                                $sOptions.qos = 1;
                            })
                     .createSubcriberOptions();
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock2";
                                $sOptions.qos = 2;
                            })
                    .createSubcriberOptions();
                })
                .create()
        .subscribeWithOptions();
        //
        mqttServerConfigApp.destroy();
    }

    /**
     * Construye un cliente basico y realiza una conexion al broker exitosa desde el cliente mqtt
     * con algunas opciones de conexion  y del buffer disponibles agregando un subcriptor a un topico mock y
     * un publicador al mismo topico enviando un mensaje
     *
     * @throws MqttException
     */
    @Test
    public void testPublisherOkWithConnection() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();
        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
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
                                $bOptions.bufferEnabled = false;
                            }).createBufferOptions();
                })
                .with($ -> {
                    $.publisherOptions = new MqttGatewayBuilder.PublisherOptionsBuilder()
                            .with($pOptions -> {
                                $pOptions.topicFilter = "/topicMock";
                                $pOptions.qos = 2;
                                $pOptions.retained = false;
                            }).createPublisherOptions();
                })
         .create();
        //
        mqttGateway.publishWithOptions("Hello Word!!!");

        // wait for send and receive message
        mqttGateway.detach(500);
        mqttServerConfigApp.destroy();
    }


    @Test
    public void testSubcriberPublisherOkWithConnection1Message() throws MqttException, InterruptedException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();
        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
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
                                $bOptions.bufferEnabled = false;
                            }).createBufferOptions();
                })
                .with($ -> {
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.qos = 0;
                                $sOptions.topicFilter = "/topicMock";
                                $sOptions.messageListener = new DefaultMqttListener();
                            }).createSubcriberOptions();
                })
                .with($ -> {
                    $.publisherOptions = new MqttGatewayBuilder.PublisherOptionsBuilder()
                            .with($pOptions -> {
                                $pOptions.topicFilter = "/topicMock";
                                $pOptions.qos = 0;
                                $pOptions.retained = false;
                            }).createPublisherOptions();
                })
                .create()
        .subscribeWithOptions();
        //
        mqttGateway.publishWithOptions("Hello Word!!!");

        // wait for send and receive message
        mqttGateway.detach(5000);
        mqttServerConfigApp.destroy();

        // check the message
        //TestSubcriberConsumer messageSubcriptor = (TestSubcriberConsumer) mqttGateway
                //.getSubscriberOptions().getOptions().stream().findFirst().get().getMessageSubcriptor();
        //assertEquals(messageSubcriptor.getMessagesReceived().size(), 1);
        //
        //String payload = new String(messageSubcriptor.getMessagesReceived().get(0).getPayload(), StandardCharsets.UTF_8);
        //String topic   = messageSubcriptor.getMessagesReceived().get(0).getTopicFilter();
        //int qos        = messageSubcriptor.getMessagesReceived().get(0).getQos();
        //
        //assertEquals(payload, "Hello Word!!!");
        //assertEquals(topic, "/topicMock");
        //assertEquals(qos, 0);

    }






    @Override
    protected void tearDown() throws Exception {

    }
}
