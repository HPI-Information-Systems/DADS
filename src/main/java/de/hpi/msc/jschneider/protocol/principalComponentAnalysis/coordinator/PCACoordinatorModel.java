package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.coordinator;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.Counter;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class PCACoordinatorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private int numberOfParticipants;
    @NonNull @Getter
    private final Counter nextParticipantIndex = new Counter(0);
    @NonNull @Getter
    private final Map<Long, ProcessorId> participantIndices = new HashMap<>();

}
