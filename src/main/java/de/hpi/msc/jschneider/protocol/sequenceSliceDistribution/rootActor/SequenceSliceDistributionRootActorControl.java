package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorRegistrationEvents;
import de.hpi.msc.jschneider.protocol.reaper.ReapedActor;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionMessages;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor.SequenceSliceDistributorControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.distributor.SequenceSliceDistributorModel;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver.SequenceSliceReceiverControl;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.receiver.SequenceSliceReceiverModel;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;

public class SequenceSliceDistributionRootActorControl extends AbstractProtocolParticipantControl<SequenceSliceDistributionRootActorModel>
{
    public SequenceSliceDistributionRootActorControl(SequenceSliceDistributionRootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                      .match(ProcessorRegistrationEvents.ProcessorJoinedEvent.class, this::onProcessorJoined)
                      .match(SequenceSliceDistributionMessages.SequenceSlicePartMessage.class, message -> forward(message, getModel().getSliceReceiver()));
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        createSliceReceiver();

        if (!message.getLocalProcessor().isMaster())
        {
            return;
        }

        subscribeToLocalEvent(ProtocolType.ProcessorRegistration, ProcessorRegistrationEvents.ProcessorJoinedEvent.class);
    }

    private void createSliceReceiver()
    {
        val model = SequenceSliceReceiverModel.builder()
                                              .build();
        val control = new SequenceSliceReceiverControl(model);
        val sliceReceiver = trySpawnChild(ProtocolParticipant.props(control));

        if (!sliceReceiver.isPresent())
        {
            getLog().error(String.format("Unable to create %1$s!", SequenceSliceReceiverControl.class.getName()));
            getModel().setSliceReceiver(ActorRef.noSender());
            return;
        }

        getModel().setSliceReceiver(sliceReceiver.get());
    }

    private void onProcessorJoined(ProcessorRegistrationEvents.ProcessorJoinedEvent message)
    {

    }

    private ActorRef createSliceDistributor(RootActorPath sliceReceiverActorSystem)
    {
        val model = SequenceSliceDistributorModel.builder()
                                                 .sliceReceiverActorSystem(sliceReceiverActorSystem)
                                                 .sequenceReader(null)
                                                 .build();
        val control = new SequenceSliceDistributorControl(model);
        val distributor = trySpawnChild(ReapedActor.props(control));
        if (!distributor.isPresent())
        {
            getLog().error(String.format("Unable to spawn %1$s!", SequenceSliceDistributorControl.class.getName()));
            return ActorRef.noSender();
        }

        return distributor.get();
    }
}
