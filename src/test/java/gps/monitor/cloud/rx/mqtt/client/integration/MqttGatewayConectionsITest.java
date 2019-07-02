package gps.monitor.cloud.rx.mqtt.client.integration;


import gps.monitor.cloud.rx.mqtt.client.bus.Bus;
import gps.monitor.cloud.rx.mqtt.client.bus.impl.MessageNativeSubBus;
import gps.monitor.cloud.rx.mqtt.client.conf.impl.MqttServerConfigApp;
import gps.monitor.cloud.rx.mqtt.client.conf.impl.PropertiesConfigApp;
import gps.monitor.cloud.rx.mqtt.client.listener.DefaultMqttListener;
import gps.monitor.cloud.rx.mqtt.client.subscriber.MessageConsumer;
import gps.monitor.cloud.rx.mqtt.client.subscriber.TestSubcriberConsumer;
import junit.framework.TestCase;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Test;
import org.netcrusher.core.reactor.NioReactor;
import org.netcrusher.tcp.TcpCrusher;
import org.netcrusher.tcp.TcpCrusherBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by daniel.carvajal on 20-06-2018.
 */
public class MqttGatewayConectionsITest extends TestCase {

    private MqttServerConfigApp mqttServerConfigApp;

    @Override
    protected void setUp() throws Exception {

    }

    @Test
    public void testDisconnectMqttGateway() throws MqttException, InterruptedException, IOException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();

        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        // message listener with bus
        DefaultMqttListener messageListener = new DefaultMqttListener();
        Bus messageNativeSubBus = new MessageNativeSubBus();
        MessageConsumer messageConsumer = new TestSubcriberConsumer();
        //
        messageNativeSubBus.subscribe(messageConsumer);
        messageListener.setMessageSubscriberBus(messageNativeSubBus);

        // proxy tcp
        NioReactor reactor = new NioReactor();
        TcpCrusher crusher = TcpCrusherBuilder.builder()
                .withReactor(reactor)
                .withBindAddress("127.0.0.1", 1884)
                .withConnectAddress("127.0.0.1", 1883)
                .buildAndOpen();

        // config mqtt gateway
        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    // config desa
                    $.host = "127.0.0.1";
                    $.port = 1884; // bind port
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                    $.retryAndWait = true; // save attach
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with($cOptions -> {
                                $cOptions.cleanSession = true;
                                $cOptions.keepAliveInterval = 60; //seg
                                $cOptions.automaticReconnect = true;
                                $cOptions.connectionTimeout = 30;
                                $cOptions.maxInflight = 1000;
                            })
                    .createConnectOptions();
                })
                .with($ -> {
                    $.memoryPersistence = false;
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with($bOptions -> {
                                $bOptions.bufferEnabled = true;
                                $bOptions.bufferSize = 1000;
                                $bOptions.persistBuffer = true;
                            })
                    .createBufferOptions();
                })
                .with($ -> {
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with($sOptions -> {
                                $sOptions.topicFilter = "/topicMock";
                                $sOptions.qos = 0;
                                $sOptions.messageListener = messageListener;
                            })
                    .createSubcriberOptions();
                })
        .create();


        assertTrue(mqttGateway.isConnected());

        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1884"); // with bind port
        assertEquals(mqttGateway.isConnected(), true);

        // publish message
        mqttGateway.publish("/topicMock", "Hello Word!!!".getBytes(), 0, false);
        Thread.sleep(5000);

        // check the message
        TestSubcriberConsumer messageSubcriptor = (TestSubcriberConsumer) messageConsumer;
        assertTrue(messageSubcriptor.getMessagesReceived().size() == 1);
        //
        String payload = new String(messageSubcriptor.getMessagesReceived().get(0).getPayload(), StandardCharsets.UTF_8);
        String topic   = messageSubcriptor.getMessagesReceived().get(0).getTopicFilter();
        int qos        = messageSubcriptor.getMessagesReceived().get(0).getQos();
        //
        assertEquals(payload, "Hello Word!!!");
        assertEquals(topic, "/topicMock");
        assertEquals(qos, 0);

        // close connection
        crusher.close();
        Thread.sleep(60000);
        // open connection
        crusher.open();

        while(!mqttGateway.isConnected());
        assertTrue(mqttGateway.isConnected());
    }


    @Test
    public void testDisconnectMqttGatewayWithSubcriptionAndPublicationMessage() throws MqttException, InterruptedException, IOException {
        // log properties
        PropertiesConfigApp propertiesConfigApp = new PropertiesConfigApp();
        propertiesConfigApp.configure();

        // start local mqtt
        mqttServerConfigApp = new MqttServerConfigApp();
        mqttServerConfigApp.configure();

        // message listener with bus
        DefaultMqttListener messageListener = new DefaultMqttListener();
        Bus messageNativeSubBus = new MessageNativeSubBus();
        MessageConsumer messageConsumer = new TestSubcriberConsumer();
        //
        messageNativeSubBus.subscribe(messageConsumer);
        messageListener.setMessageSubscriberBus(messageNativeSubBus);

        // proxy
        NioReactor reactor = new NioReactor();
        TcpCrusher crusher = TcpCrusherBuilder.builder()
                    .withReactor(reactor)
                    .withBindAddress("127.0.0.1", 1884) // bind port
                    .withConnectAddress("127.0.0.1", 1883)
                .buildAndOpen();

        // config mqtt gateway
        MqttGateway mqttGateway = MqttGatewayBuilder.getInstance()
                .with($ -> {
                    // config desa
                    $.host = "127.0.0.1";
                    $.port = 1884;
                    $.identifier = "MockIdentifier";
                })
                .with($ -> {
                    $.debug = false;
                    $.retryAndWait = true; // save attach
                })
                .with($ -> {
                    $.connectOptions = new MqttGatewayBuilder.ConnectOptionsBuilder()
                            .with(  $cOptions -> {
                                    $cOptions.cleanSession = true;
                                    $cOptions.keepAliveInterval = 60; //seg
                                    $cOptions.automaticReconnect = true;
                                    $cOptions.connectionTimeout = 30;
                                    $cOptions.maxInflight = 1000;
                    }).createConnectOptions();
                })
                .with($ -> {
                    $.memoryPersistence = false;
                    $.bufferOptions = new MqttGatewayBuilder.BufferOptionsBuilder()
                            .with(  $bOptions -> {
                                    $bOptions.bufferEnabled = true;
                                    $bOptions.bufferSize = 1000;
                                    $bOptions.persistBuffer = true;
                            })
                    .createBufferOptions();
                })
                .with($ -> {
                    $.subscriberOptions = new MqttGatewayBuilder.SubscriberOptionsBuilder()
                            .with(  $sOptions -> {
                                    $sOptions.topicFilter = "/topicMock";
                                    $sOptions.qos = 0;
                                    $sOptions.messageListener = messageListener;
                            })
                    .createSubcriberOptions();
                })
        .create();

        assertTrue(mqttGateway.isConnected());

        assertEquals(mqttGateway.getMqttAsyncClient().getClientId(), "MockIdentifier");
        assertEquals(mqttGateway.getMqttAsyncClient().getServerURI(), "tcp://127.0.0.1:1884"); // bind port
        assertEquals(mqttGateway.isConnected(), true);

        // publish message
        mqttGateway.publish("/topicMock", "Hello Word!!!".getBytes(), 0, false);
        Thread.sleep(5000);

        // check the message
        TestSubcriberConsumer messageSubcriptor = (TestSubcriberConsumer) messageConsumer;
        assertTrue(messageSubcriptor.getMessagesReceived().size() == 1);
        //
        String payload = new String(messageSubcriptor.getMessagesReceived().get(0).getPayload(), StandardCharsets.UTF_8);
        String topic   = messageSubcriptor.getMessagesReceived().get(0).getTopicFilter();
        int qos        = messageSubcriptor.getMessagesReceived().get(0).getQos();
        //
        assertEquals(payload, "Hello Word!!!");
        assertEquals(topic, "/topicMock");
        assertEquals(qos, 0);

        // close connection
        crusher.close();
        Thread.sleep(60000);
        // open connection
        crusher.open();

        while(!mqttGateway.isConnected());
        // reconnect it's ok
        assertTrue(mqttGateway.isConnected());

        //re-subcribe
        //mqttGateway.subscribeWithOptions();

        // publish new message message
        mqttGateway.publish("/topicMock", "Re-Hello Word!!!".getBytes(), 0, false);
        // wait send a receive message
        Thread.sleep(5000);

        // check the message
        messageSubcriptor = (TestSubcriberConsumer) messageConsumer;
        assertTrue(messageSubcriptor.getMessagesReceived().size() == 2);
        //
        payload = new String(messageSubcriptor.getMessagesReceived().get(1).getPayload(), StandardCharsets.UTF_8);
        topic   = messageSubcriptor.getMessagesReceived().get(1).getTopicFilter();
        qos        = messageSubcriptor.getMessagesReceived().get(1).getQos();
        //
        assertEquals(payload, "Re-Hello Word!!!");
        assertEquals(topic, "/topicMock");
        assertEquals(qos, 0);
    }

    @Override
    protected void tearDown() throws Exception {

    }
}
