package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.fileHandling.reading.SequenceReader;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class SequenceSliceDistributorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private RootActorPath sliceReceiverActorSystem;
    @NonNull @Getter
    private SequenceReader sequenceReader;
}
