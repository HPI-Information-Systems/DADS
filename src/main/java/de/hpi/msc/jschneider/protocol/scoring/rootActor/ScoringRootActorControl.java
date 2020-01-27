package de.hpi.msc.jschneider.protocol.scoring.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.bootstrap.command.MasterCommand;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.nodeCreation.NodeCreationEvents;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.scoring.ScoringMessages;
import de.hpi.msc.jschneider.protocol.scoring.receiver.ScoringReceiverControl;
import de.hpi.msc.jschneider.protocol.scoring.receiver.ScoringReceiverModel;
import de.hpi.msc.jschneider.protocol.scoring.worker.ScoringWorkerControl;
import de.hpi.msc.jschneider.protocol.scoring.worker.ScoringWorkerModel;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

import java.util.Collection;

public class ScoringRootActorControl extends AbstractProtocolParticipantControl<ScoringRootActorModel>
{
    public ScoringRootActorControl(ScoringRootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                    .match(NodeCreationEvents.ResponsibilitiesReceivedEvent.class, this::onResponsibilitiesReceived)
                    .match(ScoringMessages.OverlappingEdgeCreationOrder.class, message -> forward(message, getModel().getWorker()))
                    .match(ScoringMessages.QueryPathLengthMessage.class, message -> forward(message, getModel().getWorker()))
                    .match(ScoringMessages.InitializePathScoresTransferMessage.class, message -> forward(message, getModel().getReceiver()));
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        createWorker();

        if (!message.getLocalProcessor().isMaster())
        {
            return;
        }

        createReceiver();
        subscribeToLocalEvent(ProtocolType.NodeCreation, NodeCreationEvents.ResponsibilitiesReceivedEvent.class);
    }

    private void createWorker()
    {
        val model = ScoringWorkerModel.builder()
                                      .build();
        val control = new ScoringWorkerControl(model);
        val worker = trySpawnChild(ProtocolParticipant.props(control), "ScoringWorker");

        if (!worker.isPresent())
        {
            getLog().error("Unable to create ScoringWorker!");
            getModel().setWorker(ActorRef.noSender());
            return;
        }

        getModel().setWorker(worker.get());
    }

    private void createReceiver()
    {
        val model = ScoringReceiverModel.builder()
                                        .build();
        val control = new ScoringReceiverControl(model);
        val receiver = trySpawnChild(ProtocolParticipant.props(control), "ScoringReceiver");

        if (!receiver.isPresent())
        {
            getLog().error("Unable to create ScoringReceiver!");
            getModel().setReceiver(ActorRef.noSender());
            return;
        }

        getModel().setReceiver(receiver.get());
    }

    private void onResponsibilitiesReceived(NodeCreationEvents.ResponsibilitiesReceivedEvent message)
    {
        try
        {
            publishQueryPathLength(message.getSubSequenceResponsibilities().keySet());
        }
        finally
        {
            complete(message);
        }
    }

    private void publishQueryPathLength(Collection<ProcessorId> workerSystems)
    {
        assert SystemParameters.getCommand() instanceof MasterCommand : "Only the master may publish the query path length!";

        val queryPathLength = ((MasterCommand) SystemParameters.getCommand()).getQueryPathLength();

        for (val workerSystem : workerSystems)
        {
            val protocol = getProtocol(workerSystem, ProtocolType.Scoring);
            assert protocol.isPresent() : "Workers must implement the Scoring protocol!";

            send(ScoringMessages.QueryPathLengthMessage.builder()
                                                       .sender(getModel().getSelf())
                                                       .receiver(protocol.get().getRootActor())
                                                       .queryPathLength(queryPathLength)
                                                       .build());
        }
    }
}
