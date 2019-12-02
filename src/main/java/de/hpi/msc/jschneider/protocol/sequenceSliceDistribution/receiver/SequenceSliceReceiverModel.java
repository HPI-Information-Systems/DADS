package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver;

import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.fileHandling.writing.BinaryDirectoryWriter;
import de.hpi.msc.jschneider.fileHandling.writing.SequenceWriter;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import sun.java2d.xr.MutableInteger;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class SequenceSliceReceiverModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private final Map<Integer, float[]> sliceParts = new HashMap<>();
    @NonNull @Getter
    private final MutableInteger expectedNextSliceIndex = new MutableInteger(0);
    @NonNull @Getter @Builder.Default
    private SequenceWriter sequenceWriter = BinaryDirectoryWriter.fromDirectory(Paths.get(SystemParameters.getWorkingDirectory().toString(), "sequence-slices/").toFile());
}
