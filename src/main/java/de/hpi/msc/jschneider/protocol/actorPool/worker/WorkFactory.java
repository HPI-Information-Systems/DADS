package de.hpi.msc.jschneider.protocol.actorPool.worker;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;

public interface WorkFactory
{
    boolean hasNext();

    ActorPoolMessages.WorkMessage next(ActorRef worker);
}
