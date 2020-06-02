package de.hpi.msc.jschneider.protocol.graphMerging.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.graphMerging.GraphMergingMessages;
import de.hpi.msc.jschneider.protocol.graphMerging.merger.GraphMergerControl;
import de.hpi.msc.jschneider.protocol.graphMerging.merger.GraphMergerModel;
import de.hpi.msc.jschneider.protocol.graphMerging.partitionReceiver.GraphPartitionReceiverControl;
import de.hpi.msc.jschneider.protocol.graphMerging.partitionReceiver.GraphPartitionReceiverModel;
import de.hpi.msc.jschneider.protocol.graphMerging.partitionSender.GraphPartitionSenderControl;
import de.hpi.msc.jschneider.protocol.graphMerging.partitionSender.GraphPartitionSenderModel;
import de.hpi.msc.jschneider.protocol.graphMerging.receiver.GraphReceiverControl;
import de.hpi.msc.jschneider.protocol.graphMerging.receiver.GraphReceiverModel;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

public class GraphMergingRootActorControl extends AbstractProtocolParticipantControl<GraphMergingRootActorModel>
{
    public GraphMergingRootActorControl(GraphMergingRootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                    .match(GraphMergingMessages.InitializeEdgePartitionTransferMessage.class, message -> forward(message, getModel().getPartitionReceiver()))
                    .match(GraphMergingMessages.InitializeGraphTransferMessage.class, message -> forward(message, getModel().getGraphReceiver()));
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        createGraphReceiver();
        createPartitionSender();

        if (!message.getLocalProcessor().isMaster())
        {
            return;
        }

        createPartitionReceiver();
    }

    private void createGraphReceiver()
    {
        val model = GraphReceiverModel.builder()
                                      .build();
        val control = new GraphReceiverControl(model);
        val receiver = trySpawnChild(control, "GraphReceiver");

        if (!receiver.isPresent())
        {
            getLog().error("Unable to create GraphReceiver!");
            getModel().setGraphReceiver(ActorRef.noSender());
            return;
        }

        getModel().setGraphReceiver(receiver.get());
    }

    private void createPartitionSender()
    {
        val model = GraphPartitionSenderModel.builder()
                                             .build();
        val control = new GraphPartitionSenderControl(model);
        val sender = trySpawnChild(control, "GraphPartitionSender");

        if (!sender.isPresent())
        {
            getLog().error("Unable to create GraphPartitionSender!");
            getModel().setPartitionSender(ActorRef.noSender());
            return;
        }

        getModel().setPartitionSender(sender.get());
    }

    private void createPartitionReceiver()
    {
        createMerger();

        val model = GraphPartitionReceiverModel.builder()
                                               .graphMerger(getModel().getGraphMerger())
                                               .build();
        val control = new GraphPartitionReceiverControl(model);
        val receiver = trySpawnChild(control, "GraphPartitionReceiver");

        if (!receiver.isPresent())
        {
            getLog().error("Unable to create GraphPartitionReceiver!");
            getModel().setPartitionReceiver(ActorRef.noSender());
            return;
        }

        getModel().setPartitionReceiver(receiver.get());
    }

    private void createMerger()
    {
        val model = GraphMergerModel.builder()
                                    .build();
        val control = new GraphMergerControl(model);
        val merger = trySpawnChild(control, "GraphMerger");

        if (!merger.isPresent())
        {
            getLog().error("Unable to create GraphMerger!");
            getModel().setGraphMerger(ActorRef.noSender());
            return;
        }

        getModel().setGraphMerger(merger.get());
    }
}
