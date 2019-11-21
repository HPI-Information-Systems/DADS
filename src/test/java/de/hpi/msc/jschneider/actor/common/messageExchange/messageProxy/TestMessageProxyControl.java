package de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.actor.common.Message;
import de.hpi.msc.jschneider.actor.common.MockMessage;
import junit.framework.TestCase;
import lombok.val;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMessageProxyControl extends TestCase
{
    private ActorSystem localActorSystem;
    private ActorSystem remoteActorSystem;
    private TestActorRef self;
    private TestProbe localDispatcher;
    private TestProbe remoteDispatcher;
    private TestProbe localActor;
    private TestProbe remoteActor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localActorSystem = ActorSystem.create("Local");
        remoteActorSystem = ActorSystem.create("Remote");

        self = TestActorRef.create(localActorSystem, Props.empty(), "Local Message Proxy");
        localDispatcher = TestProbe.apply(localActorSystem);
        remoteDispatcher = TestProbe.apply(remoteActorSystem);
        localActor = TestProbe.apply(localActorSystem);
        remoteActor = TestProbe.apply(remoteActorSystem);
    }

    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();
        localActorSystem.terminate();
        remoteActorSystem.terminate();
    }

    private MessageProxyModel dummyModel()
    {
        return MessageProxyModel.builder()
                                .selfProvider(() -> self)
                                .senderProvider(ActorRef::noSender)
                                .messageDispatcherProvider(() -> localDispatcher.ref())
                                .childFactory(props -> ActorRef.noSender())
                                .remoteMessageDispatcher(remoteDispatcher.ref())
                                .watchActorCallback(actorRef ->
                                                    {
                                                    })
                                .build();

    }

    private Message enqueueMessage(MessageProxyControl control, ActorRef sender, ActorRef receiver)
    {
        return enqueueMessage(control, sender, receiver, 1, 1);
    }

    private Message enqueueMessage(MessageProxyControl control, ActorRef sender, ActorRef receiver, int expectedMessages, int expectedUncompletedMessages)
    {
        val message = MockMessage.builder()
                                 .sender(sender)
                                 .receiver(receiver)
                                 .build();

        control.onMessage(message);
        assertThat(control.getModel().getMessageQueues().get(receiver.path()).size()).isEqualTo(expectedMessages);
        assertThat(control.getModel().getMessageQueues().get(receiver.path()).numberOfUncompletedMessages()).isEqualTo(expectedUncompletedMessages);

        return message;
    }

    public void testLocalToLocalMessage()
    {
        val control = new MessageProxyControl(dummyModel());
        val message = enqueueMessage(control, localActor.ref(), localActor.ref());

        localActor.expectMsg(message);
    }

    public void testRemoteToLocalMessage()
    {
        val control = new MessageProxyControl(dummyModel());
        val message = enqueueMessage(control, remoteActor.ref(), localActor.ref());

        localActor.expectMsg(message);
    }

    public void testLocalToRemoteMessage()
    {
        val control = new MessageProxyControl(dummyModel());
        val message = enqueueMessage(control, localActor.ref(), remoteActor.ref());

        remoteDispatcher.expectMsg(message);
    }

    public void testAcknowledgeLocalToLocalMessage()
    {
        val control = new MessageProxyControl(dummyModel());
        val message = enqueueMessage(control, localActor.ref(), localActor.ref());
        localActor.expectMsg(message);

        val completed = MessageProxyMessages.MessageCompletedMessage.builder()
                                                                    .sender(localActor.ref())
                                                                    .receiver(localActor.ref())
                                                                    .acknowledgedMessageId(message.getId())
                                                                    .build();

        control.onMessageCompleted(completed);
        assertThat(control.getModel().getTotalNumberOfEnqueuedMessages().get()).isEqualTo(0);
        assertThat(control.getModel().getMessageQueues().get(localActor.ref().path()).size()).isEqualTo(0);
        assertThat(control.getModel().getMessageQueues().get(localActor.ref().path()).numberOfUncompletedMessages()).isEqualTo(0);
    }

    public void testAcknowledgeLocalToRemoteMessage()
    {
        val control = new MessageProxyControl(dummyModel());
        val message = enqueueMessage(control, localActor.ref(), remoteActor.ref());
        remoteDispatcher.expectMsg(message);

        val completed = MessageProxyMessages.MessageCompletedMessage.builder()
                                                                    .sender(remoteActor.ref())
                                                                    .receiver(localActor.ref())
                                                                    .acknowledgedMessageId(message.getId())
                                                                    .build();

        control.onMessageCompleted(completed);
        assertThat(control.getModel().getTotalNumberOfEnqueuedMessages().get()).isEqualTo(0);
        assertThat(control.getModel().getMessageQueues().get(remoteActor.ref().path()).size()).isEqualTo(0);
        assertThat(control.getModel().getMessageQueues().get(remoteActor.ref().path()).numberOfUncompletedMessages()).isEqualTo(0);
    }

    public void testForwardAcknowledgeLocalToRemote()
    {
        val control = new MessageProxyControl(dummyModel());

        val completed = MessageProxyMessages.MessageCompletedMessage.builder()
                                                                    .sender(localActor.ref())
                                                                    .receiver(remoteActor.ref())
                                                                    .acknowledgedMessageId(UUID.randomUUID())
                                                                    .build();

        control.onMessageCompleted(completed);
        remoteDispatcher.expectMsg(completed);
    }

    public void testQueueMultipleMessages()
    {
        val control = new MessageProxyControl(dummyModel());
        val firstMessage = enqueueMessage(control, localActor.ref(), remoteActor.ref());
        remoteDispatcher.expectMsg(firstMessage);

        val secondMessage = enqueueMessage(control, localActor.ref(), remoteActor.ref(), 2, 1);

        val completed = MessageProxyMessages.MessageCompletedMessage.builder()
                                                                    .sender(remoteActor.ref())
                                                                    .receiver(localActor.ref())
                                                                    .acknowledgedMessageId(firstMessage.getId())
                                                                    .build();
        control.onMessageCompleted(completed);
        assertThat(control.getModel().getMessageQueues().get(remoteActor.ref().path()).size()).isEqualTo(1);
        assertThat(control.getModel().getMessageQueues().get(remoteActor.ref().path()).numberOfUncompletedMessages()).isEqualTo(1);
        remoteDispatcher.expectMsg(secondMessage);
    }
}
