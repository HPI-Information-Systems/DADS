package de.hpi.msc.jschneider.actor.common;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.NoArgsConstructor;
import lombok.val;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Reaper extends BaseActor
{
    private static final String NAME = "Reaper";
    private static final String ACTOR_PATH = "/user/" + NAME;

    public static ActorRef spawn(final ActorSystem actorSystem)
    {
        return actorSystem.actorOf(Props.create(Reaper.class), NAME);
    }

    @NoArgsConstructor
    public static class WatchMeMessage implements Serializable
    {
        private static final long serialVersionUID = 834090848009487113L;
    }

    public static void watch(BaseActor actor)
    {
        val reaper = actor.context().system().actorSelection(ACTOR_PATH);
        reaper.tell(new WatchMeMessage(), actor.self());
    }

    private final Set<ActorRef> watchedActors = new HashSet<>();

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

        if (!watchedActors.isEmpty())
        {
            log().error("{} was stopped before every local actor has been reaped!", getClass().getName());
        }
    }

    @Override
    public Receive createReceive()
    {
        return improvedReceiveBuilder().match(WatchMeMessage.class, this::onWatchMe)
                                       .match(Terminated.class, this::onTerminated)
                                       .build();
    }

    private void onWatchMe(final WatchMeMessage message)
    {
        if (sender().path().root() != self().path().root())
        {
            log().warning("Actor of remote system tried to get watched!");
            return;
        }

        if (!watchedActors.add(sender()))
        {
            return;
        }

        context().watch(sender());
        log().debug("{} is now watching {} actors.", getClass().getName(), watchedActors.size());
    }

    private void onTerminated(final Terminated message)
    {
        context().unwatch(sender());
        if (!watchedActors.remove(sender()))
        {
            return;
        }

        log().debug("{} is now watching {} actors.", getClass().getName(), watchedActors.size());
        if (!watchedActors.isEmpty())
        {
            return;
        }

        log().info("All local actors have been reaped. Terminating system!");
        context().system().terminate();
    }

}
