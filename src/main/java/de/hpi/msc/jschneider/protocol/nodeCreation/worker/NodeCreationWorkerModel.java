package de.hpi.msc.jschneider.protocol.nodeCreation.worker;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private double maximumValue;
    @Setter @Getter
    private int numberOfIntersectionSegments;
    @Setter @Getter
    private NodeCreationMessages.ReducedSubSequenceMessage reducedSubSequenceMessage;
    @NonNull @Getter
    private final Map<Integer, List<float[]>> intersections = new HashMap<>();
}
