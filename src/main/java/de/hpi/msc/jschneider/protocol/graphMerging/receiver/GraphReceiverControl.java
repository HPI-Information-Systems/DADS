package de.hpi.msc.jschneider.protocol.graphMerging.receiver;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingEvents;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;

import java.util.Arrays;

public class GraphReceiverControl extends AbstractProtocolParticipantControl<GraphReceiverModel>
{
    public GraphReceiverControl(GraphReceiverModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(GraphMergingMessages.InitializeGraphTransferMessage.class, this::onInitializeGraphTransfer);
    }

    private void onInitializeGraphTransfer(DataTransferMessages.InitializeDataTransferMessage message)
    {
        getModel().getDataTransferManager().accept(message, dataReceiver -> dataReceiver.addSink(getModel().getGraphEdgeSink())
                                                                                        .whenFinished(this::onGraphTransferFinished));
    }

    private void onGraphTransferFinished(DataReceiver dataReceiver)
    {
        val edges = getModel().getGraphEdgeSink().edges();

        val numberOfEdges = edges.size();
        val numberOfNodes = edges.values()
                                 .stream()
                                 .flatMapToInt(edge -> Arrays.stream(new int[]{edge.getFrom().hashCode(), edge.getTo().hashCode()}))
                                 .distinct()
                                 .count();
        getLog().info("Received graph (#edges = {}, #nodes = {}).", numberOfEdges, numberOfNodes);
        trySendEvent(ProtocolType.GraphMerging, eventDispatcher -> GraphMergingEvents.GraphReceivedEvent.builder()
                                                                                                        .sender(getModel().getSelf())
                                                                                                        .receiver(eventDispatcher)
                                                                                                        .graph(edges)
                                                                                                        .build());

        isReadyToBeTerminated();
    }
}
