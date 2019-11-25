package de.hpi.msc.jschneider.actor.master.nodeRegistry;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher.MessageDispatcherMessages;
import de.hpi.msc.jschneider.actor.common.workDispatcher.WorkDispatcherMessages;
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
    private TestActorRef existingMessageDispatcher;
    private TestActorRef newMessageDispatcher;
    private TestActorRef localWorkDispatcher;
    private TestActorRef existingWorkDispatcher;
    private TestActorRef newWorkDispatcher;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localActorSystem = ActorSystem.create("local");
        existingActorSystem = ActorSystem.create("existing");
        newActorSystem = ActorSystem.create("new");

        self = TestActorRef.create(localActorSystem, Props.empty());
        localMessageDispatcher = TestProbe.apply(localActorSystem);
        existingMessageDispatcher = TestActorRef.create(existingActorSystem, Props.empty());
        newMessageDispatcher = TestActorRef.create(newActorSystem, Props.empty());
        localWorkDispatcher = TestActorRef.create(localActorSystem, Props.empty());
        existingWorkDispatcher = TestActorRef.create(existingActorSystem, Props.empty());
        newWorkDispatcher = TestActorRef.create(newActorSystem, Props.empty());
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
                                .workDispatcherProvider(() -> localWorkDispatcher)
                                .build();
    }

    private NodeRegistryMessages.RegisterWorkerNodeMessage registrationMessage(ActorRef workDispatcher, ActorRef messageDispatcher)
    {
        return NodeRegistryMessages.RegisterWorkerNodeMessage.builder()
                                                             .sender(workDispatcher)
                                                             .receiver(self)
                                                             .maximumMemory(1024 * 1024)
                                                             .numberOfWorkers(1)
                                                             .messageDispatcher(messageDispatcher)
                                                             .workDispatcher(workDispatcher)
                                                             .build();
    }

    public void testRegisterRemoteNode()
    {
        val control = new NodeRegistryControl(dummyModel());
        val registrationMessage = registrationMessage(newWorkDispatcher, newMessageDispatcher);

        control.onRegistration(registrationMessage);
        assertThat(control.getModel().getWorkerNodes().containsKey(newMessageDispatcher.path().root())).isTrue();

        // introduce the new message dispatcher to the existing message dispatchers (ours)
        val localMessage = localMessageDispatcher.expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class);
        assertThat(localMessage.getMessageDispatchers().length).isEqualTo(1);
        assertThat(localMessage.getMessageDispatchers()[0]).isEqualTo(newMessageDispatcher);

        // introduce existing message dispatchers (ours) to the new message dispatcher
        val remoteMessage = localMessageDispatcher.expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class);
        assertThat(remoteMessage.getReceiver()).isEqualTo(newMessageDispatcher);
        assertThat(remoteMessage.getMessageDispatchers().length).isEqualTo(1);
        assertThat(remoteMessage.getMessageDispatchers()[0]).isEqualTo(localMessageDispatcher.ref());

        // acknowledge registration
        localMessageDispatcher.expectMsgClass(WorkDispatcherMessages.AcknowledgeRegistrationMessage.class);
    }

    public void testRegisterRemoteNodeWithExistingNodes()
    {
        val control = new NodeRegistryControl(dummyModel());
        control.getModel().getWorkerNodes().put(existingMessageDispatcher.path().root(),
                                                WorkerNode.fromRegistrationMessage(registrationMessage(existingWorkDispatcher, existingMessageDispatcher)));
        val registrationMessage = registrationMessage(newWorkDispatcher, newMessageDispatcher);

        control.onRegistration(registrationMessage);
        assertThat(control.getModel().getWorkerNodes().containsKey(newMessageDispatcher.path().root())).isTrue();

        val messages = new ArrayList<MessageDispatcherMessages.AddMessageDispatchersMessage>();

        // introduce the new message dispatcher to the existing message dispatchers (ours + existing)
        messages.add(localMessageDispatcher.expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class));
        messages.add(localMessageDispatcher.expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class));
        assertThat(messages.stream().filter(message -> message.getReceiver() == localMessageDispatcher.ref()).count()).isEqualTo(1);
        assertThat(messages.stream().filter(message -> message.getReceiver() == existingMessageDispatcher).count()).isEqualTo(1);

        // introduce the existing message dispatcher to the new message dispatcher
        val remoteMessage = localMessageDispatcher.expectMsgClass(MessageDispatcherMessages.AddMessageDispatchersMessage.class);
        assertThat(remoteMessage.getReceiver()).isEqualTo(newMessageDispatcher);
        assertThat(remoteMessage.getMessageDispatchers().length).isEqualTo(2);

        val existingMessageDispatchers = Arrays.asList(remoteMessage.getMessageDispatchers());
        assertThat(existingMessageDispatchers.contains(localMessageDispatcher.ref())).isTrue();
        assertThat(existingMessageDispatchers.contains(existingMessageDispatcher)).isTrue();

        // acknowledge registration
        localMessageDispatcher.expectMsgClass(WorkDispatcherMessages.AcknowledgeRegistrationMessage.class);
    }
}
