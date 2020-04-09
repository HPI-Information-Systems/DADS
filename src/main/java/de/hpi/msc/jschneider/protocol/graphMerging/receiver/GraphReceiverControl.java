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

import java.util.Arrays;
import java.util.stream.Collectors;

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
        val numberOfEdges = getModel().getEdges().size();
        val numberOfNodes = getModel().getEdges()
                                      .values()
                                      .stream()
                                      .flatMapToInt(edge -> Arrays.stream(new int[]{edge.getFrom().hashCode(), edge.getTo().hashCode()}))
                                      .boxed()
                                      .collect(Collectors.toSet())
                                      .size();
        getLog().info("Received graph (#edges = {}, #nodes = {}).", numberOfEdges, numberOfNodes);
        trySendEvent(ProtocolType.GraphMerging, eventDispatcher -> GraphMergingEvents.GraphReceivedEvent.builder()
                                                                                                        .sender(getModel().getSelf())
                                                                                                        .receiver(eventDispatcher)
                                                                                                        .graph(getModel().getEdges())
                                                                                                        .build());

        isReadyToBeTerminated();
    }
}
