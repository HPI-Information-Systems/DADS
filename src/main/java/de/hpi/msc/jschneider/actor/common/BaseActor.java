package de.hpi.msc.jschneider.actor.common;

import akka.actor.AbstractLoggingActor;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

public abstract class BaseActor extends AbstractLoggingActor
{
    protected ImprovedReceiveBuilder improvedReceiveBuilder()
    {
        return new ImprovedReceiveBuilder().matchAny(this::handleAny);
    }

    protected void handleAny(Object message)
    {
        log().warning("{} received unknown message: {}", getClass().getName(), message);
    }
}
