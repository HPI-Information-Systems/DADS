package de.hpi.msc.jschneider.protocol.common.control;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

import java.util.Optional;

public interface ProtocolParticipantControl<TModel extends ProtocolParticipantModel>
{
    TModel getModel();

    void setModel(TModel model) throws NullPointerException;

    ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder);

    Optional<Protocol> getLocalProtocol(ProtocolType protocolType);

    Optional<Protocol> getProtocol(RootActorPath actorSystem, ProtocolType protocolType);

    boolean tryWatch(ActorRef subject);

    boolean tryUnwatch(ActorRef subject);

    Optional<ActorRef> trySpawnChild(Props props);

    void onAny(Object message);
}
