package de.hpi.msc.jschneider.actor.common.reaper;

import akka.actor.Props;
import akka.actor.Terminated;
import akka.testkit.TestActorRef;
import de.hpi.msc.jschneider.actor.common.ActorTest;
import de.hpi.msc.jschneider.actor.common.MockPostbox;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class ReaperControllerTest extends ActorTest
{

    private MockPostbox owner;
    private ReaperController controller;

    @Override
    protected void setUp()
    {
        super.setUp();

        owner = new MockPostbox();
        val self = TestActorRef.create(actorSystem, Props.empty(), "reaper");
        owner.setSelfFactory(() -> self);

        controller = new ReaperController(owner);
    }

    public void testDoesNotWatchRemoteActor()
    {
        val remoteSystem = createActorSystem();
        val sender = TestActorRef.create(remoteSystem, Props.empty(), "remote actor");

        owner.setSenderFactory(() -> sender);

        controller.onWatchMe(new ReaperMessages.WatchMeMessage());
        assertThat(controller.hasWatchedActors()).isEqualTo(false);
    }

    public void testWatchLocalActor()
    {
        val sender = TestActorRef.create(actorSystem, Props.empty(), "local actor");

        owner.setSenderFactory(() -> sender);

        assertThat(controller.hasWatchedActors()).isEqualTo(false);

        controller.onWatchMe(new ReaperMessages.WatchMeMessage());
        assertThat(controller.hasWatchedActors()).isEqualTo(true);
    }

    public void testTerminatedActorsAreUnwatched()
    {
        val sender = TestActorRef.create(actorSystem, Props.empty(), "local actor");

        owner.setSenderFactory(() -> sender);

        controller.onWatchMe(new ReaperMessages.WatchMeMessage());
        assertThat(controller.hasWatchedActors()).isEqualTo(true);

        controller.onTerminated(new Terminated(sender, true, true));
        assertThat(controller.hasWatchedActors()).isEqualTo(false);
    }
}
