package de.hpi.msc.jschneider.actor.common;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.LoggingAdapter;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

public abstract class BaseActor<TController extends BaseActorController> extends AbstractLoggingActor implements Postbox
{
    private final TController controller;

    protected BaseActor()
    {
        controller = createController();
    }

    protected abstract TController createController();

    protected TController controller()
    {
        return controller;
    }

    protected ImprovedReceiveBuilder improvedReceiveBuilder()
    {
        return new ImprovedReceiveBuilder().matchAny(this::handleAny);
    }

    protected void handleAny(Object message)
    {
        log().warning("{} received unknown message: {}", getClass().getName(), message);
    }

    public ActorRef spawnChild(Props props)
    {
        return context().system().actorOf(props);
    }

    public LoggingAdapter log()
    {
        return log();
    }
}
