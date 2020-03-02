package de.hpi.msc.jschneider.protocol.nodeCreation.coordinator;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SuperBuilder
public class NodeCreationCoordinatorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private final List<NodeCreationMessages.NodeCreationWorkerReadyMessage> readyMessages = new ArrayList<>();
    @Setter @Getter @Builder.Default
    private double maximumValue = Double.MIN_VALUE;
    @Getter
    private int totalNumberOfIntersectionSegments;
    @Setter @Getter
    private LocalDateTime startTime;
    @Setter @Getter
    private LocalDateTime endTime;
    @Getter
    private final Set<ProcessorId> unfinishedWorkers = new HashSet<>();
}
