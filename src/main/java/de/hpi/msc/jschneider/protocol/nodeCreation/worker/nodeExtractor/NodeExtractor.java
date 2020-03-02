package de.hpi.msc.jschneider.protocol.nodeCreation.worker.nodeExtractor;

import com.google.common.primitives.Doubles;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.math.Node;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.math.kernelDensity.GaussianKernelDensity;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.actorPool.worker.ActorPoolWorkerControl;
import de.hpi.msc.jschneider.protocol.actorPool.worker.WorkConsumer;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GenericDataSource;
import lombok.val;
import lombok.var;

import java.util.List;
import java.util.stream.Collectors;

public class NodeExtractor implements WorkConsumer
{
    @Override
    public void process(ActorPoolWorkerControl control, ActorPoolMessages.WorkMessage workLoad)
    {
        assert workLoad instanceof NodeExtractorMessages.CreateNodeCollectionMessage : "Unexpected workload!";
        process(control, (NodeExtractorMessages.CreateNodeCollectionMessage) workLoad);
    }

    private void process(ActorPoolWorkerControl control, NodeExtractorMessages.CreateNodeCollectionMessage message)
    {
        val intersections = combineIntersections(message.getIntersections());
        val h = Calculate.scottsFactor(intersections.length, 1L);


        val kde = new GaussianKernelDensity(intersections, h);
        val probabilities = kde.evaluate(message.getDensitySamples());
        val localMaximumIndices = Calculate.localMaximumIndices(probabilities);
        val nodeCollection = NodeCollection.builder()
                                           .intersectionSegment(message.getIntersectionSegment())
                                           .build();
        for (val localMaximumIndex : localMaximumIndices)
        {
            nodeCollection.getNodes().add(Node.builder()
                                              .intersectionLength(message.getDensitySamples()[localMaximumIndex])
                                              .build());
        }

        publish(control, message, nodeCollection);
    }

    private double[] combineIntersections(List<double[]> intersectionParts)
    {
        val numberOfIntersections = intersectionParts.stream().mapToInt(intersections -> intersections.length).sum();
        val intersections = new double[numberOfIntersections];
        var startIndex = 0;
        for (var i = 0; startIndex < intersections.length; ++i)
        {
            val part = intersectionParts.get(i);
            System.arraycopy(part, 0, intersections, startIndex, part.length);
            startIndex += part.length;
        }

        return intersections;
    }

    private void publish(ActorPoolWorkerControl control, NodeExtractorMessages.CreateNodeCollectionMessage message, NodeCollection nodeCollection)
    {
        control.send(NodeExtractorMessages.NodeCollectionCreatedMessage.builder()
                                                                       .sender(control.getModel().getSelf())
                                                                       .receiver(message.getSender())
                                                                       .nodeCollection(nodeCollection)
                                                                       .build());

        val nodes = Doubles.toArray(nodeCollection.getNodes().stream().map(Node::getIntersectionLength).collect(Collectors.toList()));
        for (val participant : message.getParticipants())
        {
            val protocol = control.getProtocol(participant, ProtocolType.EdgeCreation);
            assert protocol.isPresent() : "Node creation processors must also implement the Edge creation protocol!";

            control.getModel().getDataTransferManager().transfer(GenericDataSource.create(nodes),
                                                                 (dataDistributor, operationId) -> NodeCreationMessages.InitializeNodesTransferMessage.builder()
                                                                                                                                                      .sender(dataDistributor)
                                                                                                                                                      .receiver(protocol.get().getRootActor())
                                                                                                                                                      .operationId(operationId)
                                                                                                                                                      .intersectionSegment(nodeCollection.getIntersectionSegment())
                                                                                                                                                      .build());
        }
    }
}
