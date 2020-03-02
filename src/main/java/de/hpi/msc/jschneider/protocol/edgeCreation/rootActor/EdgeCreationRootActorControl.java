package de.hpi.msc.jschneider.protocol.edgeCreation.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.edgeCreation.EdgeCreationMessages;
import de.hpi.msc.jschneider.protocol.edgeCreation.worker.EdgeCreationWorkerControl;
import de.hpi.msc.jschneider.protocol.edgeCreation.worker.EdgeCreationWorkerModel;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

public class EdgeCreationRootActorControl extends AbstractProtocolParticipantControl<EdgeCreationRootActorModel>
{
    public EdgeCreationRootActorControl(EdgeCreationRootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                    .match(NodeCreationMessages.InitializeNodesTransferMessage.class, message -> forward(message, getModel().getWorker()))
                    .match(EdgeCreationMessages.LastNodeMessage.class, message -> forward(message, getModel().getWorker()));
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        createWorker();
    }

    private void createWorker()
    {
        val model = EdgeCreationWorkerModel.builder()
                                           .build();
        val control = new EdgeCreationWorkerControl(model);
        val worker = trySpawnChild(ProtocolParticipant.props(control), "EdgeCreationWorker");

        if (!worker.isPresent())
        {
            getLog().error("Unable to spawn EdgeCreationWorker!");
            getModel().setWorker(ActorRef.noSender());
            return;
        }

        getModel().setWorker(worker.get());
    }
}
