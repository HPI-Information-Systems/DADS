package de.hpi.msc.jschneider.protocol.nodeCreation.worker;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuperBuilder
public class NodeCreationWorkerModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private MatrixStore<Double> reducedProjection;
    @Setter @Getter
    private long firstSubSequenceIndex;
    @Setter @Getter
    private boolean isLastSubSequenceChunk;
    @Setter @Getter
    private Map<ActorRef, Int32Range> intersectionSegmentResponsibilities;
    @Setter @Getter
    private Map<ActorRef, Int64Range> subSequenceResponsibilities;
    @Setter @Getter
    private Set<ProcessorId> participants;
    @Setter @Getter
    private double maximumValue;
    @Setter @Getter
    private int numberOfIntersectionSegments;
    @Setter @Getter
    private double[] densitySamples;
    @Setter @Getter
    private NodeCreationMessages.ReducedSubSequenceMessage reducedSubSequenceMessage;
    @NonNull @Getter
    private final Map<Integer, List<double[]>> intersections = new HashMap<>();
    @NonNull @Getter
    private final Set<IntersectionCollection[]> intersectionCollections = new HashSet<>();
    @Setter @Getter
    private int expectedNumberOfIntersectionCollections;
    @NonNull @Getter
    private final Map<Integer, NodeCollection> nodeCollections = new HashMap<>();
    @Getter @Setter
    private LocalDateTime startTime;
    @Getter @Setter
    private LocalDateTime endTime;
}
