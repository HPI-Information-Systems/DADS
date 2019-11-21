package de.hpi.msc.jschneider.actor.common.reaper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.testkit.TestActorRef;
import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestReaperControl extends TestCase
{
    private ActorSystem localActorSystem;
    private ActorSystem remoteActorSystem;
    private TestActorRef self;
    private TestActorRef localActor;
    private TestActorRef remoteActor;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        localActorSystem = ActorSystem.create("Local");
        remoteActorSystem = ActorSystem.create("Remote");
        self = TestActorRef.create(localActorSystem, Props.empty());
        localActor = TestActorRef.create(localActorSystem, Props.empty());
        remoteActor = TestActorRef.create(remoteActorSystem, Props.empty());
    }

    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();
        localActorSystem.terminate();
        remoteActorSystem.terminate();
    }

    private ReaperModel dummyModel()
    {
        return ReaperModel.builder()
                          .selfProvider(() -> self)
                          .senderProvider(ActorRef::noSender)
                          .messageDispatcherProvider(ActorRef::noSender)
                          .childFactory(props -> ActorRef.noSender())
                          .watchActorCallback(actorRef ->
                                              {
                                              })
                          .terminateSystemCallback(() -> localActorSystem.terminate())
                          .build();
    }

    private ReaperModel dummyModelWithSender(ActorRef sender)
    {
        return ReaperModel.builder()
                          .selfProvider(() -> self)
                          .senderProvider(() -> sender)
                          .messageDispatcherProvider(ActorRef::noSender)
                          .childFactory(props -> ActorRef.noSender())
                          .watchActorCallback(actorRef ->
                                              {
                                              })
                          .terminateSystemCallback(() -> localActorSystem.terminate())
                          .build();
    }

    private void watch(ReaperControl control, ActorRef actor, int expectedWatchedActors)
    {

        control.onWatchMe(ReaperMessages.WatchMeMessage.builder()
                                                       .sender(actor)
                                                       .receiver(self)
                                                       .build());

        assertThat(control.getModel().getChildActors().isEmpty()).isTrue();
        assertThat(control.getModel().getWatchedActors().size()).isEqualTo(expectedWatchedActors);
    }

    public void testWatchMe()
    {
        val control = new ReaperControl(dummyModel());
        watch(control, localActor, 1);
    }

    public void testDoNotWatchTwice()
    {
        val control = new ReaperControl(dummyModel());
        watch(control, localActor, 1);
        watch(control, localActor, 1);
    }

    public void testDoNotWatchRemoteActor()
    {
        val control = new ReaperControl(dummyModel());
        watch(control, remoteActor, 0);
    }

    public void testOnTerminated() throws InterruptedException
    {
        val control = new ReaperControl(dummyModelWithSender(localActor));
        val terminatedFuture = localActorSystem.whenTerminated();

        watch(control, localActor, 1);

        control.onTerminated(Terminated.apply(localActor, true, true));

        assertThat(control.getModel().getWatchedActors().isEmpty()).isTrue();

        Thread.sleep(1000);
        assertThat(terminatedFuture.isCompleted()).isTrue();
    }
}
