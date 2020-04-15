package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.MatrixSink;
import de.hpi.msc.jschneider.utility.matrix.MatrixBuilder;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;
import java.util.List;

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
    private MatrixSink projectionSink;
    @Setter @Getter
    private LocalDateTime startTime;
    @Setter @Getter
    private LocalDateTime endTime;
}
