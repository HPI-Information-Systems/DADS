package de.hpi.msc.jschneider.protocol.graphMerging.receiver;

import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingEvents;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataReceiver;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.val;

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
        getModel().getDataTransferManager().accept(message, dataReceiver -> dataReceiver.whenDataPartReceived(this::onGraphPartReceived)
                                                                                        .whenFinished(this::onGraphTransferFinished));
    }

    private void onGraphPartReceived(DataTransferMessages.DataPartMessage message)
    {
        val edges = Serialize.toGraphEdges(message.getPart());

        for (val edge : edges)
        {
            getModel().getEdges().put(edge.hashCode(), edge);
        }
    }

    private void onGraphTransferFinished(DataReceiver dataReceiver)
    {
        getLog().info(String.format("Received graph (#edges = %1$d).", getModel().getEdges().size()));
        trySendEvent(ProtocolType.GraphMerging, eventDispatcher -> GraphMergingEvents.GraphReceivedEvent.builder()
                                                                                                        .sender(getModel().getSelf())
                                                                                                        .receiver(eventDispatcher)
                                                                                                        .graph(getModel().getEdges())
                                                                                                        .build());
    }
}
