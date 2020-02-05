package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.coordinator;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.HashSet;
import java.util.Set;

@SuperBuilder
public class PCACoordinatorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private int numberOfParticipants;
    @NonNull @Getter @Builder.Default
    private Set<ProcessorId> participants = new HashSet<>();
}
