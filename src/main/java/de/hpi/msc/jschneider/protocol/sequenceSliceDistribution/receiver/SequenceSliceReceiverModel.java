package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.SequenceMatrixSink;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

@SuperBuilder
public class SequenceSliceReceiverModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private long firstSubSequenceIndex;
    @Setter @Getter
    private boolean isLastSubSequenceChunk;
    @Setter @Getter
    private int subSequenceLength;
    @Setter @Getter
    private int convolutionSize;
    @Setter @Getter
    private SequenceMatrixSink projectionSink;
    @Setter @Getter
    private LocalDateTime startTime;
    @Setter @Getter
    private LocalDateTime endTime;
}
