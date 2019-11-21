package de.hpi.msc.jschneider.actor.utility.actorPool;

import akka.actor.ActorRef;

public interface ActorPool
{
    ActorRef[] getActors();

    ActorRef getActor();
}
