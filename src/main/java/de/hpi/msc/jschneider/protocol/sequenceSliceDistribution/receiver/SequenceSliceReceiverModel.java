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

@SuperBuilder
public class SequenceSliceReceiverModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private long firstSubSequenceIndex;
    @Setter @Getter
    private int subSequenceLength;
    @Setter @Getter
    private int convolutionSize;
    @Setter @Getter
    private MatrixInitializer projectionInitializer;
    @Setter @Getter @Builder.Default
    private float[] unusedRecords = new float[0];
    @NonNull @Getter @Builder.Default
    private SequenceWriter sequenceWriter = BinaryDirectoryWriter.fromDirectory(Paths.get(SystemParameters.getWorkingDirectory().toString(), "sequence-slices/").toFile());
}
