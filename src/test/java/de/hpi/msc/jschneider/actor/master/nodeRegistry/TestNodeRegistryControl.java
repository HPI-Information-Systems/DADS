package de.hpi.msc.jschneider.actor.master.nodeRegistry;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.actor.ActorTestCase;
import de.hpi.msc.jschneider.actor.TestNode;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher.MessageDispatcherMessages;
import de.hpi.msc.jschneider.actor.common.workDispatcher.WorkDispatcherMessages;
import lombok.val;

import java.util.ArrayList;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TestNodeRegistryControl extends ActorTestCase
{
    private TestNode existingNode;
    private TestNode newNode;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        existingNode = spawnNode("existing");
        newNode = spawnNode("new");
    }

    private NodeRegistryModel dummyModel()
    {
        return NodeRegistryModel.builder()
                                .selfProvider(() -> self.ref())
                                .senderProvider(ActorRef::noSender)
                                .messageDispatcherProvider(() -> localNode.getMessageDispatcher().ref())
                                .childFactory(props -> ActorRef.noSender())
                                .watchActorCallback(actorRef ->
                                                    {
                                                    })
                                .workDispatcherProvider(() -> localNode.getWorkDispatcher().ref())
                                .build();
    }

    private NodeRegistryMessages.RegisterWorkerNodeMessage registrationMessage(TestNode node)
    {
        return NodeRegistryMessages.RegisterWorkerNodeMessage.builder()
                                                             .sender(node.getWorkDispatcher().ref())
                                                             .receiver(self.ref())
                                                             .maximumMemory(1024 * 1024)
                                                             .numberOfWorkers(1)
                                                             .messageDispatcher(node.getMessageDispatcher().ref())
                                                             .workDispatcher(node.getWorkDispatcher().ref())
                                                             .build();
    }

    public void testRegisterRemoteNode()
    {
        val control = new NodeRegistryControl(dummyModel());
        val registrationMessage = registrationMessage(newNode);

        control.onRegistration(registrationMessage);
        assertThat(control.getModel().getWorkerNodes().containsKey(newNode.rootPath())).isTrue();

        // introduce the new message dispatcher to the existing message dispatchers (ours)
        val localMessage = localNode.getMessageDispatcher().expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class);
        assertThat(localMessage.getMessageDispatchers().length).isEqualTo(1);
        assertThat(localMessage.getMessageDispatchers()[0]).isEqualTo(newNode.getMessageDispatcher().ref());

        // introduce existing message dispatchers (ours) to the new message dispatcher
        val remoteMessage = localNode.getMessageDispatcher().expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class);
        assertThat(remoteMessage.getReceiver()).isEqualTo(newNode.getMessageDispatcher().ref());
        assertThat(remoteMessage.getMessageDispatchers().length).isEqualTo(1);
        assertThat(remoteMessage.getMessageDispatchers()[0]).isEqualTo(localNode.getMessageDispatcher().ref());

        // acknowledge registration
        localNode.getMessageDispatcher().expectMsgClass(WorkDispatcherMessages.AcknowledgeRegistrationMessage.class);
    }

    public void testRegisterRemoteNodeWithExistingNodes()
    {
        val control = new NodeRegistryControl(dummyModel());
        control.getModel().getWorkerNodes().put(existingNode.rootPath(),
                                                WorkerNode.fromRegistrationMessage(registrationMessage(existingNode)));
        val registrationMessage = registrationMessage(newNode);

        control.onRegistration(registrationMessage);
        assertThat(control.getModel().getWorkerNodes().containsKey(newNode.rootPath())).isTrue();

        val messages = new ArrayList<MessageDispatcherMessages.AddMessageDispatchersMessage>();

        // introduce the new message dispatcher to the existing message dispatchers (ours + existing)
        messages.add(localNode.getMessageDispatcher().expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class));
        messages.add(localNode.getMessageDispatcher().expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class));
        assertThat(messages.stream().filter(message -> message.getReceiver() == localNode.getMessageDispatcher().ref()).count()).isEqualTo(1);
        assertThat(messages.stream().filter(message -> message.getReceiver() == existingNode.getMessageDispatcher().ref()).count()).isEqualTo(1);

        // introduce the existing message dispatcher to the new message dispatcher
        val remoteMessage = localNode.getMessageDispatcher().expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class);
        assertThat(remoteMessage.getReceiver()).isEqualTo(newNode.getMessageDispatcher().ref());
        assertThat(remoteMessage.getMessageDispatchers().length).isEqualTo(2);

        val existingMessageDispatchers = Arrays.asList(remoteMessage.getMessageDispatchers());
        assertThat(existingMessageDispatchers.contains(localNode.getMessageDispatcher().ref())).isTrue();
        assertThat(existingMessageDispatchers.contains(existingNode.getMessageDispatcher().ref())).isTrue();

        // acknowledge registration
        localNode.getMessageDispatcher().expectMsgClass(WorkDispatcherMessages.AcknowledgeRegistrationMessage.class);
    }
}
