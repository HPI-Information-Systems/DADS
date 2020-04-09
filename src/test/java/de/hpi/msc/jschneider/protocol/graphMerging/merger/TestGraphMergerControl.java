package de.hpi.msc.jschneider.protocol.graphMerging.merger;

import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import lombok.val;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGraphMergerControl extends ProtocolTestCase
{
    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.GraphMerging};
    }

    private GraphMergerModel dummyModel()
    {
        return finalizeModel(GraphMergerModel.builder()
                                             .build());
    }

    private GraphMergerControl control()
    {
        return new GraphMergerControl(dummyModel());
    }

    public void testMergeGradually()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val edges = createGraphEdges("{0_0} -[3]-> {1_1}",
                                     "{1_1} -[2]-> {0_0}",
                                     "{1_1} -[1]-> {2_2}",
                                     "{2_2} -[1]-> {0_0}",
                                     "{0_0} -[2]-> {1_1}");

        val firstPart = Arrays.copyOfRange(edges, 0, edges.length / 2);
        val secondPart = Arrays.copyOfRange(edges, edges.length / 2, edges.length);

        val firstMessage = GraphMergingMessages.EdgesReceivedMessage.builder()
                                                                    .sender(self.ref())
                                                                    .receiver(self.ref())
                                                                    .edges(firstPart)
                                                                    .build();
        messageInterface.apply(firstMessage);

        assertThat(control.getModel().getEdges().values().stream().map(GraphEdge::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder("{0_0} -[3]-> {1_1}",
                                           "{1_1} -[2]-> {0_0}");

        assertThatMessageIsCompleted(firstMessage);

        val secondMessage = GraphMergingMessages.EdgesReceivedMessage.builder()
                                                                     .sender(self.ref())
                                                                     .receiver(self.ref())
                                                                     .edges(secondPart)
                                                                     .build();
        messageInterface.apply(secondMessage);

        assertThat(control.getModel().getEdges().values().stream().map(GraphEdge::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder("{0_0} -[5]-> {1_1}",
                                           "{1_1} -[2]-> {0_0}",
                                           "{1_1} -[1]-> {2_2}",
                                           "{2_2} -[1]-> {0_0}");

        assertThatMessageIsCompleted(secondMessage);

        val allEdgesReceivedMessage = GraphMergingMessages.AllEdgesReceivedMessage.builder()
                                                                                  .sender(self.ref())
                                                                                  .receiver(self.ref())
                                                                                  .workerSystems(new ProcessorId[]{localProcessor.getId()})
                                                                                  .build();
        messageInterface.apply(allEdgesReceivedMessage);

        val initializeGraphTransfer = localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(GraphMergingMessages.InitializeGraphTransferMessage.class);
        assertThat(initializeGraphTransfer.getReceiver()).isEqualTo(localProcessor.getProtocolRootActor(ProtocolType.GraphMerging).ref());

        assertThatMessageIsCompleted(allEdgesReceivedMessage);
    }
}
