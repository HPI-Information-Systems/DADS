package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
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
    private final Map<ProcessorId, ActorRef> sliceDistributors = new HashMap<>();
    @NonNull @Getter
    private SequenceSliceDistributorFactory sliceDistributorFactory;
    @Setter @Getter
    private ActorRef sliceReceiver;
}
