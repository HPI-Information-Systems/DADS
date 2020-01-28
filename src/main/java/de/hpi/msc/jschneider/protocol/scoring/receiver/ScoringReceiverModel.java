package de.hpi.msc.jschneider.protocol.scoring.receiver;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuperBuilder
public class ScoringReceiverModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private int subSequenceLength;
    @NonNull @Getter @Builder.Default
    private final Set<ProcessorId> runningDataTransfers = new HashSet<>();
    @Setter @Getter
    private Map<ProcessorId, Int64Range> subSequenceResponsibilities;
    @NonNull @Getter
    private final Map<ProcessorId, double[]> pathScores = new HashMap<>();
    @Setter @Getter @Builder.Default
    private boolean responsibilitiesReceived = false;
}
