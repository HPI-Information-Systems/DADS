package de.hpi.msc.jschneider.protocol.common.control;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.msc.jschneider.protocol.common.model.ProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

import java.util.Optional;

public interface ProtocolParticipantControl<TModel extends ProtocolParticipantModel>
{
    TModel getModel();

    void setModel(TModel model) throws NullPointerException;

    ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder);

    boolean tryWatch(ActorRef subject);

    boolean tryUnwatch(ActorRef subject);

    Optional<ActorRef> trySpawnChild(Props props);

    void onAny(Object message);
}
