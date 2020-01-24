package de.hpi.msc.jschneider.protocol.graphMerging.receiver;

import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingEvents;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGraphReceiverControl extends ProtocolTestCase
{
    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.GraphMerging};
    }

    private GraphReceiverModel dummyModel()
    {
        return finalizeModel(GraphReceiverModel.builder()
                                               .build());
    }

    private GraphReceiverControl control()
    {
        return new GraphReceiverControl(dummyModel());
    }

    private String initializeGraphTransfer(PartialFunction<Object, BoxedUnit> messageInterface)
    {
        val operationId = UUID.randomUUID().toString();
        val initializeGraphTransfer = GraphMergingMessages.InitializeGraphTransferMessage.builder()
                                                                                         .sender(self.ref())
                                                                                         .receiver(self.ref())
                                                                                         .operationId(operationId)
                                                                                         .build();
        messageInterface.apply(initializeGraphTransfer);

        localProcessor.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(DataTransferMessages.RequestNextDataPartMessage.class);
        assertThatMessageIsCompleted(initializeGraphTransfer);

        return operationId;
    }

    public void testAcceptGraphTransfer()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        initializeGraphTransfer(messageInterface);
    }

    public void testPublishGraphReceivedEvent()
    {
        val control = control();
        val messageInterface = createMessageInterface(control);

        val operationId = initializeGraphTransfer(messageInterface);

        val edges = createGraphEdges("{0_0} -[3]-> {1_1}",
                                     "{1_1} -[2]-> {0_0}");
        val partMessage = DataTransferMessages.DataPartMessage.builder()
                                                              .sender(self.ref())
                                                              .receiver(self.ref())
                                                              .part(Serialize.toBytes(edges))
                                                              .operationId(operationId)
                                                              .isLastPart(true)
                                                              .build();
        messageInterface.apply(partMessage);

        val graphReceivedEvent = expectEvent(GraphMergingEvents.GraphReceivedEvent.class);
        assertThat(graphReceivedEvent.getGraph().values()).containsExactlyInAnyOrder(edges);
    }
}
