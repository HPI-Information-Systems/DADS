package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor;

import de.hpi.msc.jschneider.fileHandling.reading.SequenceReader;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class SequenceSliceDistributorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private ProcessorId sliceReceiverActorSystem;
    @NonNull @Getter
    private SequenceReader sequenceReader;
    @Getter
    private long firstSubSequenceIndex;
    @Getter
    private boolean isLastSubSequenceChunk;
    @Getter
    private int subSequenceLength;
    @Getter
    private int convolutionSize;
}
