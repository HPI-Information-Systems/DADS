package de.hpi.msc.jschneider.protocol.common.eventDispatcher;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;

import java.util.Map;
import java.util.Set;

public interface EventDispatcherModel extends ProtocolParticipantModel
{
    Map<Class<?>, Set<ActorRef>> getEventSubscribers();

    void setEventSubscribers(Map<Class<?>, Set<ActorRef>> subscribers);
}
