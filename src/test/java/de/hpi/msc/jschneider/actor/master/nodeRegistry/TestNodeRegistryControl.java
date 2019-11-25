package de.hpi.msc.jschneider.actor.master.nodeRegistry;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher.MessageDispatcherMessages;
import junit.framework.TestCase;
import lombok.val;

import java.util.ArrayList;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TestNodeRegistryControl extends TestCase
{
    private ActorSystem localActorSystem;
    private ActorSystem existingActorSystem;
    private ActorSystem newActorSystem;
    private TestActorRef self;
    private TestProbe localMessageDispatcher;
    private TestProbe existingMessageDispatcher;
    private TestProbe newMessageDispatcher;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localActorSystem = ActorSystem.create("local");
        existingActorSystem = ActorSystem.create("existing");
        newActorSystem = ActorSystem.create("new");

        self = TestActorRef.create(localActorSystem, Props.empty());
        localMessageDispatcher = TestProbe.apply(localActorSystem);
        existingMessageDispatcher = TestProbe.apply(existingActorSystem);
        newMessageDispatcher = TestProbe.apply(newActorSystem);
    }

    @Override
    public void tearDown()
    {
        localActorSystem.terminate();
        existingActorSystem.terminate();
        newActorSystem.terminate();
    }

    private NodeRegistryModel dummyModel()
    {
        return NodeRegistryModel.builder()
                                .selfProvider(() -> self)
                                .senderProvider(ActorRef::noSender)
                                .messageDispatcherProvider(() -> localMessageDispatcher.ref())
                                .childFactory(props -> ActorRef.noSender())
                                .watchActorCallback(actorRef ->
                                                    {
                                                    })
                                .workDispatcherProvider(ActorRef::noSender)
                                .build();
    }

    private NodeRegistryMessages.RegisterWorkerNodeMessage registrationMessage(ActorRef messageDispatcher)
    {
        return NodeRegistryMessages.RegisterWorkerNodeMessage.builder()
                                                             .sender(messageDispatcher)
                                                             .receiver(self)
                                                             .maximumMemory(1024 * 1024)
                                                             .numberOfWorkers(1)
                                                             .messageDispatcher(messageDispatcher)
                                                             .workerDispatcher(ActorRef.noSender())
                                                             .build();
    }

    public void testRegisterRemoteNode()
    {
        val control = new NodeRegistryControl(dummyModel());
        val registrationMessage = registrationMessage(newMessageDispatcher.ref());

        control.onRegistration(registrationMessage);
        assertThat(control.getModel().getWorkerNodes().containsKey(newMessageDispatcher.ref().path().root())).isTrue();

        // introduce the new message dispatcher to the existing message dispatchers (ours)
        val localMessage = localMessageDispatcher.expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class);
        assertThat(localMessage.getMessageDispatchers().length).isEqualTo(1);
        assertThat(localMessage.getMessageDispatchers()[0]).isEqualTo(newMessageDispatcher.ref());

        // introduce existing message dispatchers (ours) to the new message dispatcher
        val remoteMessage = localMessageDispatcher.expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class);
        assertThat(remoteMessage.getReceiver()).isEqualTo(newMessageDispatcher.ref());
        assertThat(remoteMessage.getMessageDispatchers().length).isEqualTo(1);
        assertThat(remoteMessage.getMessageDispatchers()[0]).isEqualTo(localMessageDispatcher.ref());
    }

    public void testRegisterRemoteNodeWithExistingNodes()
    {
        val control = new NodeRegistryControl(dummyModel());
        control.getModel().getWorkerNodes().put(existingMessageDispatcher.ref().path().root(),
                                                WorkerNode.fromRegistrationMessage(registrationMessage(existingMessageDispatcher.ref())));
        val registrationMessage = registrationMessage(newMessageDispatcher.ref());

        control.onRegistration(registrationMessage);
        assertThat(control.getModel().getWorkerNodes().containsKey(newMessageDispatcher.ref().path().root())).isTrue();

        val messages = new ArrayList<MessageDispatcherMessages.AddMessageDispatchersMessage>();

        // introduce the new message dispatcher to the existing message dispatchers (ours + existing)
        messages.add(localMessageDispatcher.expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class));
        messages.add(localMessageDispatcher.expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class));
        assertThat(messages.stream().filter(message -> message.getReceiver() == localMessageDispatcher.ref()).count()).isEqualTo(1);
        assertThat(messages.stream().filter(message -> message.getReceiver() == existingMessageDispatcher.ref()).count()).isEqualTo(1);

        // introduce the existing message dispatcher to the new message dispatcher
        val remoteMessage = localMessageDispatcher.expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class);
        assertThat(remoteMessage.getReceiver()).isEqualTo(newMessageDispatcher.ref());
        assertThat(remoteMessage.getMessageDispatchers().length).isEqualTo(2);

        val existingMessageDispatchers = Arrays.asList(remoteMessage.getMessageDispatchers());
        assertThat(existingMessageDispatchers.contains(localMessageDispatcher.ref())).isTrue();
        assertThat(existingMessageDispatchers.contains(existingMessageDispatcher.ref())).isTrue();
    }
}
