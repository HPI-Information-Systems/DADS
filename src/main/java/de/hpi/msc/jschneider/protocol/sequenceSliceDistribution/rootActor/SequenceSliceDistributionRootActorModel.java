package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class SequenceSliceDistributionRootActorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private final Map<RootActorPath, ActorRef> sliceDistributors = new HashMap<>();
    @Setter @Getter
    private ActorRef sliceReceiver;
}
