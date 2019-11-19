package de.hpi.msc.jschneider.actor.common.reaper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.msc.jschneider.actor.common.BaseActor;
import lombok.val;

public class Reaper extends BaseActor<ReaperController>
{
    private static final String NAME = "Reaper";
    private static final String ACTOR_PATH = "/user/" + NAME;

    public static ActorRef spawnIn(final ActorSystem actorSystem)
    {
        return actorSystem.actorOf(Props.create(Reaper.class), NAME);
    }

    public static void watch(BaseActor actor)
    {
        val reaper = actor.context().system().actorSelection(ACTOR_PATH);
        reaper.tell(new ReaperMessages.WatchMeMessage(), actor.self());
    }

    @Override
    public void preStart() throws Exception
    {
        super.preStart();
        log().debug("%1$s has been started.", self());
    }

    @Override
    public void postStop() throws Exception
    {
        super.postStop();

        if (controller().hasWatchedActors())
        {
            log().error("{} was stopped before every local actor has been reaped!", getClass().getName());
        }
    }

    @Override
    protected ReaperController createController()
    {
        return new ReaperController(this);
    }

    @Override
    public Receive createReceive()
    {
        return improvedReceiveBuilder().match(ReaperMessages.WatchMeMessage.class, controller()::onWatchMe)
                                       .match(Terminated.class, this::onTerminated)
                                       .build();
    }

    private void onTerminated(final Terminated message)
    {
        controller().onTerminated(message);
        if (controller().hasWatchedActors())
        {
            return;
        }

        log().info("All local actors have been reaped. Terminating system!");
        context().system().terminate();
    }

    @Override
    public void watch(ActorRef actor)
    {
        context().watch(actor);
    }

    @Override
    public void unwatch(ActorRef actor)
    {
        context().unwatch(actor);
    }
}
