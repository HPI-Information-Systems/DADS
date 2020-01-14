package de.hpi.msc.jschneider.protocol.nodeCreation.worker;

import akka.actor.ActorRef;
import com.google.common.primitives.Floats;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.math.Intersection;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.math.Node;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.val;
import lombok.var;
import org.ojalgo.function.aggregator.Aggregator;
import smile.stat.distribution.KernelDensity;

import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

public class NodeCreationWorkerControl extends AbstractProtocolParticipantControl<NodeCreationWorkerModel>
{
    private static final int NUMBER_OF_DENSITY_SAMPLES = 250;

    public NodeCreationWorkerControl(NodeCreationWorkerModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(DimensionReductionEvents.ReducedProjectionCreatedEvent.class, this::onReducedProjectionCreated)
                    .match(NodeCreationMessages.InitializeNodeCreationMessage.class, this::onInitializeNodeCreation)
                    .match(NodeCreationMessages.IntersectionsAtAngleMessage.class, this::onIntersectionsAtAngle);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        subscribeToLocalEvent(ProtocolType.DimensionReduction, DimensionReductionEvents.ReducedProjectionCreatedEvent.class);
    }

    private void onReducedProjectionCreated(DimensionReductionEvents.ReducedProjectionCreatedEvent message)
    {
        try
        {
            getModel().setReducedProjection(message.getReducedProjection());
            getModel().setFirstSubSequenceIndex(message.getFirstSubSequenceIndex());
            getModel().setLastSubSequenceChunk(message.isLastSubSequenceChunk());
            val maximumValue = Math.max(getModel().getReducedProjection().aggregateAll(Aggregator.MAXIMUM),
                                        -getModel().getReducedProjection().aggregateAll(Aggregator.MINIMUM));
            val subSequenceIndices = Int64Range.builder()
                                               .start(getModel().getFirstSubSequenceIndex())
                                               .end(getModel().getFirstSubSequenceIndex() + getModel().getReducedProjection().countColumns())
                                               .build();

            val protocol = getMasterProtocol(ProtocolType.NodeCreation);
            if (!protocol.isPresent())
            {
                getLog().error("Unable to perform three-way-handshake, because master protocol (NodeCreationCoordinator) is missing!");
                return;
            }

            send(NodeCreationMessages.NodeCreationWorkerReadyMessage.builder()
                                                                    .sender(getModel().getSelf())
                                                                    .receiver(protocol.get().getRootActor())
                                                                    .subSequenceIndices(subSequenceIndices)
                                                                    .maximumValue(maximumValue)
                                                                    .isLastSubSequenceChunk(getModel().isLastSubSequenceChunk())
                                                                    .build());
        }
        finally
        {
            complete(message);
        }
    }

    private void onInitializeNodeCreation(NodeCreationMessages.InitializeNodeCreationMessage message)
    {
        try
        {
            getModel().setSampleResponsibilities(message.getSampleResponsibilities());
            getModel().setMaximumValue(message.getMaximumValue());
            getModel().setDensitySamples(Calculate.makeRange(0.0d, getModel().getMaximumValue(), getModel().getMaximumValue() / NUMBER_OF_DENSITY_SAMPLES));

            val intersectionCollections = Calculate.intersections(getModel().getReducedProjection(), message.getNumberOfSamples());
            for (val intersectionCollection : intersectionCollections)
            {
                sendIntersections(intersectionCollection);
            }
        }
        finally
        {
            complete(message);
        }
    }

    private void sendIntersections(IntersectionCollection intersectionCollection)
    {
        val responsibleProcessor = responsibleProcessor(intersectionCollection.getIntersectionPointIndex());

        val intersections = Floats.toArray(intersectionCollection.getIntersections().stream().map(Intersection::getVectorLength).collect(Collectors.toList()));

        send(NodeCreationMessages.IntersectionsAtAngleMessage.builder()
                                                             .sender(getModel().getSelf())
                                                             .receiver(responsibleProcessor)
                                                             .intersectionPointIndex(intersectionCollection.getIntersectionPointIndex())
                                                             .intersections(intersections)
                                                             .build());
    }

    private ActorRef responsibleProcessor(int intersectionIndex)
    {
        val processorRootPath = getModel().getSampleResponsibilities().entrySet()
                                          .stream()
                                          .filter(keyValuePair -> keyValuePair.getValue().contains(intersectionIndex))
                                          .map(Map.Entry::getKey)
                                          .findFirst();

        assert processorRootPath.isPresent() : String.format("Unable to find responsible processor for intersection index %1$d!", intersectionIndex);
        return processorRootPath.get();
    }

    private void onIntersectionsAtAngle(NodeCreationMessages.IntersectionsAtAngleMessage message)
    {
        try
        {
            val myResponsibilities = getModel().getSampleResponsibilities().get(getModel().getSelf());
            assert myResponsibilities.contains(message.getIntersectionPointIndex()) : "Received intersections for an angle we are not responsible for!";

            getModel().getIntersections().putIfAbsent(message.getIntersectionPointIndex(), new ArrayList<>());
            getModel().getIntersections().get(message.getIntersectionPointIndex()).add(message.getIntersections());

            createNodes(message.getIntersectionPointIndex());
        }
        finally
        {
            complete(message);
        }
    }

    private void createNodes(int intersectionPointIndex)
    {
        val intersectionList = getModel().getIntersections().get(intersectionPointIndex);
        if (intersectionList.size() != getModel().getSampleResponsibilities().size())
        {
            // not all processor sent their intersections yet
            return;
        }

        val numberOfIntersections = intersectionList.stream().mapToInt(intersections -> intersections.length).sum();
        val intersections = new double[numberOfIntersections];
        var startIndex = 0;
        for (var i = 0; startIndex < intersections.length; ++i)
        {
            val part = intersectionList.get(i);
            System.arraycopy(toDoubles(part), 0, intersections, startIndex, part.length);
            startIndex += part.length;
        }

        val h = Calculate.scottsFactor(intersections.length, 1L);
        val kde = new KernelDensity(intersections, h);
        val probabilities = new double[NUMBER_OF_DENSITY_SAMPLES];

        assert getModel().getDensitySamples().length == probabilities.length : "|densitySamples| != |probabilities|";

        for (var i = 0; i < getModel().getDensitySamples().length; ++i)
        {
            probabilities[i] = kde.p(getModel().getDensitySamples()[i]);
        }

        val localMaximumIndices = Calculate.localMaximumIndices(probabilities);
        val nodeCollection = NodeCollection.builder()
                                           .intersectionPointIndex(intersectionPointIndex)
                                           .build();
        for (val localMaximumIndex : localMaximumIndices)
        {
            nodeCollection.getNodes().add(Node.builder()
                                              .intersectionLength(getModel().getDensitySamples()[localMaximumIndex])
                                              .probability(probabilities[localMaximumIndex])
                                              .build());
        }

        trySendEvent(ProtocolType.NodeCreation, eventDispatcher -> NodeCreationEvents.NodesCreatedEvent.builder()
                                                                                                       .sender(getModel().getSelf())
                                                                                                       .receiver(eventDispatcher)
                                                                                                       .nodeCollection(nodeCollection)
                                                                                                       .build());
    }

    private double[] toDoubles(float[] floats)
    {
        val result = new double[floats.length];
        for (var i = 0; i < floats.length; ++i)
        {
            result[i] = floats[i];
        }

        return result;
    }
}
