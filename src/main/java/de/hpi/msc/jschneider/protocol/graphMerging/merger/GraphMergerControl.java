package de.hpi.msc.jschneider.protocol.graphMerging.merger;

import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GenericDataSource;
import lombok.val;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

public class GraphMergerControl extends AbstractProtocolParticipantControl<GraphMergerModel>
{
    public GraphMergerControl(GraphMergerModel model)
    {
        super(model);
    }

    @Override
    public void preStart()
    {
        super.preStart();

        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(NodeCreationEvents.ResponsibilitiesReceivedEvent.class, this::onResponsibilitiesReceived)
                    .match(GraphMergingMessages.EdgesReceivedMessage.class, this::onEdgesReceived)
                    .match(GraphMergingMessages.AllEdgesReceivedMessage.class, this::onAllEdgesReceived);
    }

    private void onResponsibilitiesReceived(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        try
        {
            assert getModel().getSubSequenceResponsibilities() == null : "Responsibilities were received already!";

            getModel().setSubSequenceResponsibilities(new HashMap<>(message.getSubSequenceResponsibilities()));
        }
        finally
        {
            complete(message);
        }
    }

    private void onEdgesReceived(GraphMergingMessages.EdgesReceivedMessage message)
    {
        try
        {
            for (val edge : message.getEdges())
            {
                addEdge(edge);
            }
        }
        finally
        {
            complete(message);
        }
    }

    private void addEdge(GraphEdge edge)
    {
        val hash = edge.hashCode();
        val existingEdge = getModel().getEdges().get(hash);

        if (existingEdge == null)
        {
            getModel().getEdges().put(hash, edge);
        }
        else
        {
            existingEdge.setWeight(existingEdge.getWeight() + edge.getWeight());
        }
    }

    private void onAllEdgesReceived(GraphMergingMessages.AllEdgesReceivedMessage message)
    {
        try
        {
            val numberOfEdges = getModel().getEdges().size();
            val numberOfNodes = getModel().getEdges()
                                          .values()
                                          .stream()
                                          .flatMap(edge -> Arrays.stream(new Integer[]{edge.getFrom().hashCode(), edge.getTo().hashCode()}))
                                          .collect(Collectors.toSet())
                                          .size();
            val totalEdgeWeights = getModel().getEdges()
                                             .values()
                                             .stream()
                                             .mapToLong(GraphEdge::getWeight)
                                             .sum();

            getLog().info("================================================================================================");
            getLog().info("================================================================================================");
            getLog().info(String.format("Graph merged: %1$d edges (tot. weight: %2$d), %3$d nodes.", numberOfEdges, totalEdgeWeights, numberOfNodes));
            getLog().info("================================================================================================");
            getLog().info("================================================================================================");

//            Debug.print(getModel().getEdges().values().toArray(new GraphEdge[0]), String.format("%1$s-graph.txt", ProcessorId.of(getModel().getSelf())));

            publishGraph(message.getWorkerSystems());
        }
        finally
        {
            complete(message);
        }
    }

    private void publishGraph(ProcessorId[] workerSystems)
    {
        for (val workerSystem : workerSystems)
        {
            val protocol = getProtocol(workerSystem, ProtocolType.GraphMerging);
            assert protocol.isPresent()
                    : String.format("Unable to transfer graph to %1$s, because the processor does not implement the required protocol!", workerSystem);

            getModel().getDataTransferManager().transfer(GenericDataSource.create(getModel().getEdges().values().toArray(new GraphEdge[0])),
                                                         (dataDistributor, operationId) -> GraphMergingMessages.InitializeGraphTransferMessage.builder()
                                                                                                                                              .sender(dataDistributor)
                                                                                                                                              .receiver(protocol.get().getRootActor())
                                                                                                                                              .operationId(operationId)
                                                                                                                                              .build());
        }
    }
}
