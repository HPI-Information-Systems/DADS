package de.hpi.msc.jschneider.protocol.messageExchange.messageProxy;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.TestMessage;
import de.hpi.msc.jschneider.protocol.TestProcessor;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import lombok.val;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMessageProxyControl extends ProtocolTestCase
{
    private TestProbe localActor;
    private TestProcessor remoteProcessor;
    private TestProbe remoteActor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localActor = localProcessor.createActor("actor");
        remoteProcessor = createSlave();
        remoteActor = remoteProcessor.createActor("actor");
    }

    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange};
    }

    private MessageProxyModel model(TestProcessor connectedProcessor)
    {
        return finalizeModel(MessageProxyModel.builder()
                                              .messageDispatcher(connectedProcessor.getProtocol(ProtocolType.MessageExchange).getRootActor())
                                              .build());
    }

    private MessageProxyControl control(TestProcessor connectedProcessor)
    {
        return new MessageProxyControl(model(connectedProcessor));
    }

    private TestMessage enqueueMessage(MessageProxyControl control, PartialFunction<Object, BoxedUnit> messageInterface, TestProbe sender, TestProbe receiver, int expectedMessageQueueSize, int expectedUncompletedMessages)
    {
        val message = TestMessage.builder()
                                 .sender(sender.ref())
                                 .receiver(receiver.ref())
                                 .build();
        messageInterface.apply(message);

        if (ProcessorId.of(receiver.ref()).equals(ProcessorId.of(localActor.ref())))
        {
            assertThat(control.getModel().getMessageQueues().get(receiver.ref().path()).size()).isEqualTo(expectedMessageQueueSize);
            assertThat(control.getModel().getMessageQueues().get(receiver.ref().path()).numberOfUncompletedMessages()).isEqualTo(expectedUncompletedMessages);
        }

        return message;
    }

    private void completeMessage(MessageProxyControl control, PartialFunction<Object, BoxedUnit> messageInterface, MessageExchangeMessages.MessageExchangeMessage message, int expectedMessageQueueSize, int expectedUncompletedMessages)
    {
        val completedMessage = MessageExchangeMessages.MessageCompletedMessage.builder()
                                                                              .sender(message.getReceiver())
                                                                              .receiver(message.getSender())
                                                                              .completedMessageId(message.getId())
                                                                              .build();
        messageInterface.apply(completedMessage);

        assertThat(control.getModel().getMessageQueues().get(message.getReceiver().path()).size()).isEqualTo(expectedMessageQueueSize);
        assertThat(control.getModel().getMessageQueues().get(message.getReceiver().path()).numberOfUncompletedMessages()).isEqualTo(expectedUncompletedMessages);
    }

    public void testFirstMessageToLocalReceiverIsSentImmediately()
    {
        val control = control(localProcessor);
        val messageInterface = createMessageInterface(control);

        val message = enqueueMessage(control, messageInterface, localActor, localActor, 1, 1);

        assertThat(localActor.expectMsgClass(TestMessage.class)).isSameAs(message);
    }

    public void testSecondMessageIsQueuedUntilFirstMessageIsCompleted()
    {
        val control = control(localProcessor);
        val messageInterface = createMessageInterface(control);

        val firstMessage = enqueueMessage(control, messageInterface, localActor, localActor, 1, 1);
        assertThat(localActor.expectMsgClass(TestMessage.class)).isSameAs(firstMessage);

        val secondMessage = enqueueMessage(control, messageInterface, localActor, localActor, 2, 1);
        localActor.expectNoMessage();

        completeMessage(control, messageInterface, firstMessage, 1, 1);
        assertThat(localActor.expectMsgClass(TestMessage.class)).isSameAs(secondMessage);
    }

    public void testMessageToRemoteReceiverIsForwardedToRemoteDispatcher()
    {
        val control = control(remoteProcessor);
        val messageInterface = createMessageInterface(control);

        val message = enqueueMessage(control, messageInterface, localActor, remoteActor, 1, 1);

        assertThat(remoteProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(TestMessage.class)).isEqualTo(message);
    }

    public void testSenderIsBackPressuredWhenReceiverQueueIsFull()
    {
        val control = control(remoteProcessor);
        control.getModel().setSingleQueueBackPressureThreshold(0);
        val messageInterface = createMessageInterface(control);

        val firstMessage = enqueueMessage(control, messageInterface, remoteActor, localActor, 1, 1);
        assertThat(localActor.expectMsgClass(TestMessage.class)).isEqualTo(firstMessage);

        val secondMessage = enqueueMessage(control, messageInterface, remoteActor, localActor, 2, 1);
        remoteProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(MessageExchangeMessages.BackPressureMessage.class);
    }

    public void testDoNotBackPressureRemoteActors()
    {
        val control = control(remoteProcessor);
        control.getModel().setSingleQueueBackPressureThreshold(0);
        val messageInterface = createMessageInterface(control);

        val remoteToLocalMessage = enqueueMessage(control, messageInterface, remoteActor, localActor, 1, 1);
        assertThat(localActor.expectMsgClass(TestMessage.class)).isSameAs(remoteToLocalMessage);
        remoteProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectNoMessage();
        remoteActor.expectNoMessage();
    }
}
