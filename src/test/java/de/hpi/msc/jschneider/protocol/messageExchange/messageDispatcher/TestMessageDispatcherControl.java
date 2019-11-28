package de.hpi.msc.jschneider.protocol.messageExchange.messageDispatcher;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestMessage;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMessageDispatcherControl extends ProtocolTestCase
{
    private TestProbe localActor;
    private TestProbe localToLocalMessageProxy;
    private TestProbe localToRemoteMessageProxy;
    private TestProcessor remoteProcessor;
    private TestProbe remoteActor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localActor = localProcessor.createActor("actor");
        localToLocalMessageProxy = localProcessor.createActor("localToLocalMessageProxy");
        localToRemoteMessageProxy = localProcessor.createActor("localToRemoteMessageProxy");
        remoteProcessor = createProcessor("remote");
        remoteActor = remoteProcessor.createActor("actor");
    }

    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange};
    }


    private MessageDispatcherControl control(TestProcessor... knownProcessors)
    {
        val model = connectedModel(knownProcessors);
        return new MessageDispatcherControl(model);
    }

    private MessageDispatcherModel connectedModel(TestProcessor... knownProcessors)
    {
        val model = finalizeModel(MessageDispatcherModel.builder().build());
        model.setProcessorProvider(actorSystem ->
                                   {
                                       for (val processor : knownProcessors)
                                       {
                                           if (processor.getRootPath().equals(actorSystem))
                                           {
                                               return processor;
                                           }
                                       }

                                       return null;
                                   });

        return model;
    }

    public void testCreateMessageProxyForNewConnection()
    {
        val control = control(remoteProcessor);
        control.getModel().setChildFactory(props -> localToRemoteMessageProxy.ref());
        val messageInterface = messageInterface(control);

        val message = TestMessage.builder()
                                 .sender(localActor.ref())
                                 .receiver(remoteActor.ref())
                                 .build();
        messageInterface.apply(message);

        assertThat(control.getModel().getMessageProxies().size()).isEqualTo(1);
        val forwardedMessage = localToRemoteMessageProxy.expectMsgClass(TestMessage.class);
        assertThat(forwardedMessage).isSameAs(message);
    }

    public void testChooseSameMessageProxyForBothDirections()
    {
        val control = control(remoteProcessor);
        control.getModel().getMessageProxies().put(remoteProcessor.getRootPath(), localToRemoteMessageProxy.ref());
        val messageInterface = messageInterface(control);

        val localToRemoteMessage = TestMessage.builder()
                                              .sender(localActor.ref())
                                              .receiver(remoteActor.ref())
                                              .build();
        messageInterface.apply(localToRemoteMessage);
        assertThat(localToRemoteMessageProxy.expectMsgClass(TestMessage.class)).isSameAs(localToRemoteMessage);

        val remoteToLocalMessage = TestMessage.builder()
                                              .sender(remoteActor.ref())
                                              .receiver(localActor.ref())
                                              .build();
        messageInterface.apply(remoteToLocalMessage);
        assertThat(localToRemoteMessageProxy.expectMsgClass(TestMessage.class)).isSameAs(remoteToLocalMessage);
    }

    public void testDoNotCreateMessageProxyForUnknownConnection()
    {
        val control = control();
        control.getModel().setChildFactory(props -> localToRemoteMessageProxy.ref());
        val messageInterface = messageInterface(control);

        val message = TestMessage.builder()
                                 .sender(localActor.ref())
                                 .receiver(remoteActor.ref())
                                 .build();
        messageInterface.apply(message);

        assertThat(control.getModel().getMessageProxies()).isEmpty();
    }

    public void testLocalMessagesAreAlsoProxied()
    {
        val control = control(localProcessor);
        control.getModel().setChildFactory(props -> localToLocalMessageProxy.ref());
        val messageInterface = messageInterface(control);

        val message = TestMessage.builder()
                                 .sender(localActor.ref())
                                 .receiver(localActor.ref())
                                 .build();
        messageInterface.apply(message);

        assertThat(control.getModel().getMessageProxies().size()).isEqualTo(1);
        val forwardedMessage = localToLocalMessageProxy.expectMsgClass(TestMessage.class);
        assertThat(forwardedMessage).isSameAs(message);
    }
}
