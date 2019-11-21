package de.hpi.msc.jschneider.actor.common.messageExchange.messageDispatcher;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.actor.common.MockMessage;
import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMessageDispatcherControl extends TestCase
{
    private ActorSystem localActorSystem;
    private ActorSystem remoteActorSystem;
    private TestActorRef self;
    private TestActorRef remoteActor;
    private TestProbe localMessageProxy;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localActorSystem = ActorSystem.create("Local");
        remoteActorSystem = ActorSystem.create("Remote");
        self = TestActorRef.create(localActorSystem, Props.empty(), "Message Dispatcher");
        remoteActor = TestActorRef.create(remoteActorSystem, Props.empty(), "Remote Actor");
        localMessageProxy = TestProbe.apply(localActorSystem);
    }

    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();

        localActorSystem.terminate();
    }

    private MessageDispatcherModel dummyModel()
    {
        return MessageDispatcherModel.builder()
                                     .selfProvider(() -> self)
                                     .senderProvider(ActorRef::noSender)
                                     .messageDispatcherProvider(() -> self)
                                     .childFactory(props -> localMessageProxy.ref())
                                     .watchActorCallback(actorRef ->
                                                         {
                                                         })
                                     .build();
    }

    private MessageDispatcherModel modelWithActualChildFactory()
    {
        return MessageDispatcherModel.builder()
                                     .selfProvider(() -> self)
                                     .senderProvider(ActorRef::noSender)
                                     .messageDispatcherProvider(() -> self)
                                     .childFactory(localActorSystem::actorOf)
                                     .watchActorCallback(actorRef ->
                                                         {
                                                         })
                                     .build();
    }

    public void testInitialize()
    {
        val control = new MessageDispatcherControl(dummyModel());

        assertThat(control.getModel().getMessageDispatchers().size()).isEqualTo(1);
        assertThat(control.getModel().getMessageDispatchers().get(self.path().root())).isEqualTo(self);
        assertThat(control.getModel().getMessageProxies().size()).isEqualTo(0);
    }

    public void testGetNewMessageProxy()
    {
        val control = new MessageDispatcherControl(dummyModel());
        val messageProxy = control.getMessageProxy(MockMessage.builder()
                                                              .sender(self)
                                                              .receiver(self)
                                                              .build());

        assertThat(messageProxy).isNotNull();
        assertThat(messageProxy).isEqualTo(localMessageProxy.ref());
        assertThat(control.getModel().getMessageProxies().size()).isEqualTo(1);
    }

    public void testGetExistingMessageProxy()
    {
        val model = dummyModel();
        model.getMessageProxies().put(self.path().root(), localMessageProxy.ref());

        val control = new MessageDispatcherControl(model);
        val messageProxy = control.getMessageProxy(MockMessage.builder()
                                                              .sender(self)
                                                              .receiver(self)
                                                              .build());

        assertThat(messageProxy).isNotNull();
        assertThat(messageProxy).isEqualTo(localMessageProxy.ref());
        assertThat(control.getModel().getMessageProxies().size()).isEqualTo(1);
    }

    public void testAddRemoteMessageDispatcher()
    {
        val control = new MessageDispatcherControl(dummyModel());
        val addRemoteDispatcher = MessageDispatcherMessages.AddMessageDispatchersMessage.builder()
                                                                                        .sender(remoteActor)
                                                                                        .receiver(self)
                                                                                        .messageDispatchers(new ActorRef[]{remoteActor})
                                                                                        .build();

        control.onAddMessageDispatchers(addRemoteDispatcher);

        assertThat(control.getModel().getMessageDispatchers().size()).isEqualTo(2);
        assertThat(control.getModel().getMessageDispatchers().get(remoteActor.path().root())).isEqualTo(remoteActor);
    }

    public void testGetMessageProxyIsIndifferentOfDirection()
    {
        val control = new MessageDispatcherControl(modelWithActualChildFactory());
        control.getModel().getMessageDispatchers().put(remoteActor.path().root(), remoteActor);

        val localToRemoteMessage = MockMessage.builder()
                                              .sender(self)
                                              .receiver(remoteActor)
                                              .build();
        val remoteToLocalMessage = MockMessage.builder()
                                              .sender(remoteActor)
                                              .receiver(self)
                                              .build();

        val messageProxy1 = control.getMessageProxy(localToRemoteMessage);
        assertThat(messageProxy1).isNotNull();
        assertThat(messageProxy1).isNotEqualTo(ActorRef.noSender());

        val messageProxy2 = control.getMessageProxy(remoteToLocalMessage);
        assertThat(messageProxy2).isNotNull();
        assertThat(messageProxy2).isNotEqualTo(ActorRef.noSender());

        assertThat(messageProxy1).isEqualTo(messageProxy2);
    }

    public void testForwardMessage()
    {
        val control = new MessageDispatcherControl(dummyModel());
        val message = MockMessage.builder()
                                 .sender(self)
                                 .receiver(self)
                                 .build();
        control.onMessage(message);
        localMessageProxy.expectMsg(message);
    }
}
