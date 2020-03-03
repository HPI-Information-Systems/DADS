package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.fileHandling.writing.BinaryDirectoryWriter;
import de.hpi.msc.jschneider.fileHandling.writing.SequenceWriter;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.SequenceMatrixInitializer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.nio.file.Paths;
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
    @Builder.Default @Getter @Setter
    private double minimumRecord = Double.MAX_VALUE;
    @Builder.Default @Getter @Setter
    private double maximumRecord = Double.MIN_VALUE;
    @Setter @Getter
    private SequenceMatrixInitializer projectionInitializer;
    @Setter @Getter @Builder.Default
    private double[] unusedRecords = new double[0];
    @Setter @Getter
    private List<Double> rawSubSequence;
    @NonNull @Getter @Builder.Default
    private SequenceWriter sequenceWriter = BinaryDirectoryWriter.fromDirectory(Paths.get(SystemParameters.getWorkingDirectory().toString(), "sequence-slices/").toFile());
    @Setter @Getter
    private LocalDateTime startTime;
    @Setter @Getter
    private LocalDateTime endTime;
}
