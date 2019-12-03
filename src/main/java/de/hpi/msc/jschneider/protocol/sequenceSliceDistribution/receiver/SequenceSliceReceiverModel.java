package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.fileHandling.writing.BinaryDirectoryWriter;
import de.hpi.msc.jschneider.fileHandling.writing.SequenceWriter;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.agrona.collections.MutableLong;

import java.nio.file.Paths;

@SuperBuilder
public class SequenceSliceReceiverModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private final MutableLong nextSubSequenceIndex = new MutableLong(0L);
    @Setter @Getter
    private int subSequenceLength;
    @NonNull @Getter @Builder.Default
    private SequenceWriter sequenceWriter = BinaryDirectoryWriter.fromDirectory(Paths.get(SystemParameters.getWorkingDirectory().toString(), "sequence-slices/").toFile());
}
