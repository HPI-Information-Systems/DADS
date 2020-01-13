package de.hpi.msc.jschneider.protocol.nodeCreation.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationMessages;
import de.hpi.msc.jschneider.protocol.nodeCreation.coordinator.NodeCreationCoordinatorControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.coordinator.NodeCreationCoordinatorModel;
import de.hpi.msc.jschneider.protocol.nodeCreation.worker.NodeCreationWorkerControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.worker.NodeCreationWorkerModel;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

public class NodeCreationRootActorControl extends AbstractProtocolParticipantControl<NodeCreationRootActorModel>
{
    public NodeCreationRootActorControl(NodeCreationRootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                    .match(NodeCreationMessages.NodeCreationWorkerReadyMessage.class, message -> forward(message, getModel().getCoordinator()));
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        createWorker();

        if (!message.getLocalProcessor().isMaster())
        {
            return;
        }

        createCoordinator();
    }

    private void createWorker()
    {
        val model = NodeCreationWorkerModel.builder()
                                           .build();
        val control = new NodeCreationWorkerControl(model);

        val worker = trySpawnChild(ProtocolParticipant.props(control), "NodeCreationWorker");

        if (!worker.isPresent())
        {
            getLog().error("Unable to spawn NodeCreationWorker!");
            getModel().setWorker(ActorRef.noSender());
            return;
        }

        getModel().setWorker(worker.get());
    }

    private void createCoordinator()
    {
        assert SystemParameters.getCommand() instanceof MasterCommand : "Only the master node may create a NodeCreationCoordinator!";

        val numberOfSamples = ((MasterCommand) SystemParameters.getCommand()).getNumberOfSamples();

        val model = NodeCreationCoordinatorModel.builder()
                                                .totalNumberOfSamples(numberOfSamples)
                                                .build();
        val control = new NodeCreationCoordinatorControl(model);

        val coordinator = trySpawnChild(ProtocolParticipant.props(control), "NodeCreationCoordinator");

        if (!coordinator.isPresent())
        {
            getLog().error("Unable to spawn NodeCreationCoordinator!");
            getModel().setCoordinator(ActorRef.noSender());
            return;
        }

        getModel().setCoordinator(coordinator.get());
    }
}
