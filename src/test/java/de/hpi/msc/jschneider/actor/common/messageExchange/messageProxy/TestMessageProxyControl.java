package de.hpi.msc.jschneider.actor.common.messageExchange.messageProxy;

import akka.actor.ActorRef;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.actor.ActorTestCase;
import de.hpi.msc.jschneider.actor.TestNode;
import de.hpi.msc.jschneider.actor.common.Message;
import de.hpi.msc.jschneider.actor.common.MockMessage;
import lombok.val;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMessageProxyControl extends ActorTestCase
{
    private TestNode remoteNode;
    private TestProbe localActor;
    private TestProbe remoteActor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        remoteNode = spawnNode("remote");
        localActor = TestProbe.apply(localNode.getActorSystem());
        remoteActor = TestProbe.apply(remoteNode.getActorSystem());
    }

    private MessageProxyModel dummyModel()
    {
        return MessageProxyModel.builder()
                                .selfProvider(() -> self.ref())
                                .senderProvider(ActorRef::noSender)
                                .messageDispatcherProvider(() -> localNode.getMessageDispatcher().ref())
                                .childFactory(props -> ActorRef.noSender())
                                .remoteMessageDispatcher(remoteNode.getMessageDispatcher().ref())
                                .watchActorCallback(actorRef ->
                                                    {
                                                    })
                                .build();
    }

    private MessageProxyModel dummyModelWithInstantBackPressure()
    {
        return MessageProxyModel.builder()
                                .selfProvider(() -> self.ref())
                                .senderProvider(ActorRef::noSender)
                                .messageDispatcherProvider(() -> localNode.getMessageDispatcher().ref())
                                .childFactory(props -> ActorRef.noSender())
                                .remoteMessageDispatcher(remoteNode.getMessageDispatcher().ref())
                                .watchActorCallback(actorRef ->
                                                    {
                                                    })
                                .singleQueueBackPressureThreshold(0)
                                .totalQueueBackPressureThreshold(0)
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

        remoteNode.getMessageDispatcher().expectMsg(message);
    }

    public void testCompleteLocalToLocalMessage()
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

    public void testCompleteLocalToRemoteMessage()
    {
        val control = new MessageProxyControl(dummyModel());
        val message = enqueueMessage(control, localActor.ref(), remoteActor.ref());
        remoteNode.getMessageDispatcher().expectMsg(message);

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

    public void testForwardCompletionLocalToRemote()
    {
        val control = new MessageProxyControl(dummyModel());

        val completed = MessageProxyMessages.MessageCompletedMessage.builder()
                                                                    .sender(localActor.ref())
                                                                    .receiver(remoteActor.ref())
                                                                    .acknowledgedMessageId(UUID.randomUUID())
                                                                    .build();

        control.onMessageCompleted(completed);
        remoteNode.getMessageDispatcher().expectMsg(completed);
    }

    public void testQueueMultipleMessages()
    {
        val control = new MessageProxyControl(dummyModel());
        val firstMessage = enqueueMessage(control, localActor.ref(), remoteActor.ref());
        remoteNode.getMessageDispatcher().expectMsg(firstMessage);

        val secondMessage = enqueueMessage(control, localActor.ref(), remoteActor.ref(), 2, 1);

        val completed = MessageProxyMessages.MessageCompletedMessage.builder()
                                                                    .sender(remoteActor.ref())
                                                                    .receiver(localActor.ref())
                                                                    .acknowledgedMessageId(firstMessage.getId())
                                                                    .build();
        control.onMessageCompleted(completed);
        assertThat(control.getModel().getMessageQueues().get(remoteActor.ref().path()).size()).isEqualTo(1);
        assertThat(control.getModel().getMessageQueues().get(remoteActor.ref().path()).numberOfUncompletedMessages()).isEqualTo(1);
        remoteNode.getMessageDispatcher().expectMsg(secondMessage);
    }

    public void testApplyBackPressure()
    {
        val control = new MessageProxyControl(dummyModelWithInstantBackPressure());
        val firstMessage = enqueueMessage(control, localActor.ref(), remoteActor.ref());
        remoteNode.getMessageDispatcher().expectMsg(firstMessage);
        localActor.expectMsgClass(MessageProxyMessages.BackPressureMessage.class);
    }

    public void testDoNotApplyBackPressureToRemoteActors()
    {
        val control = new MessageProxyControl(dummyModelWithInstantBackPressure());
        val firstMessage = enqueueMessage(control, remoteActor.ref(), localActor.ref());
        localActor.expectMsg(firstMessage);

        assertThat(control.getModel().getMessageQueues().get(remoteActor.ref().path())).isNull();
    }
}
