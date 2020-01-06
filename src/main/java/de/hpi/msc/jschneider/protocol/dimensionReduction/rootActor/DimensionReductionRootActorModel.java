package de.hpi.msc.jschneider.protocol.dimensionReduction.rootActor;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class DimensionReductionRootActorModel extends AbstractProtocolParticipantModel
{
    @Getter
    private final Map<RootActorPath, ActorRef> distributors = new HashMap<>();
    @Getter @Setter
    private ActorRef receiver;
}
