package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.fileHandling.writing.BinaryDirectoryWriter;
import de.hpi.msc.jschneider.fileHandling.writing.SequenceWriter;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.nio.file.Paths;
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
    private float minimumRecord = Float.MAX_VALUE;
    @Builder.Default @Getter @Setter
    private float maximumRecord = Float.MIN_VALUE;
    @Setter @Getter
    private MatrixInitializer projectionInitializer;
    @Setter @Getter @Builder.Default
    private float[] unusedRecords = new float[0];
    @Setter @Getter
    private List<Float> rawSubSequence;
    @NonNull @Getter @Builder.Default
    private SequenceWriter sequenceWriter = BinaryDirectoryWriter.fromDirectory(Paths.get(SystemParameters.getWorkingDirectory().toString(), "sequence-slices/").toFile());
}
