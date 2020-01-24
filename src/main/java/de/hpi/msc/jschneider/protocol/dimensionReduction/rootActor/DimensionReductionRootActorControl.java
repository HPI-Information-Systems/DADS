package de.hpi.msc.jschneider.protocol.dimensionReduction.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.ProtocolParticipant;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.DimensionReductionMessages;
import de.hpi.msc.jschneider.protocol.dimensionReduction.distributor.DimensionReductionDistributorControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.distributor.DimensionReductionDistributorModel;
import de.hpi.msc.jschneider.protocol.dimensionReduction.receiver.DimensionReductionReceiverControl;
import de.hpi.msc.jschneider.protocol.dimensionReduction.receiver.DimensionReductionReceiverModel;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.PCAEvents;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;
import lombok.val;
import org.ojalgo.matrix.store.MatrixStore;

public class DimensionReductionRootActorControl extends AbstractProtocolParticipantControl<DimensionReductionRootActorModel>
{
    public DimensionReductionRootActorControl(DimensionReductionRootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return super.complementReceiveBuilder(builder)
                    .match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp)
                    .match(PCAEvents.PrincipalComponentsCreatedEvent.class, this::onPrincipalComponentsCreated)
                    .match(DimensionReductionMessages.InitializePrincipalComponentsTransferMessage.class, message -> forward(message, getModel().getReceiver()))
                    .match(DimensionReductionMessages.InitializeRotationTransferMessage.class, message -> forward(message, getModel().getReceiver()));
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {
        createReceiver();

        if (!message.getLocalProcessor().isMaster())
        {
            return;
        }

        subscribeToLocalEvent(ProtocolType.PrincipalComponentAnalysis, PCAEvents.PrincipalComponentsCreatedEvent.class);
    }

    private void onPrincipalComponentsCreated(PCAEvents.PrincipalComponentsCreatedEvent message)
    {
        try
        {
            assert getModel().getLocalProcessor().isMaster() : "Local processor must be master in order to distribute principal components!";
            createDistributors(message.getPrincipalComponents(), message.getRotation());
        }
        finally
        {
            complete(message);
        }
    }

    private void createReceiver()
    {
        val model = DimensionReductionReceiverModel.builder().build();
        val control = new DimensionReductionReceiverControl(model);
        val receiver = trySpawnChild(ProtocolParticipant.props(control), "DimensionReductionReceiver");

        if (!receiver.isPresent())
        {
            getLog().error(String.format("Unable to create %1$s!", DimensionReductionReceiverControl.class.getName()));
            getModel().setReceiver(ActorRef.noSender());
            return;
        }

        getModel().setReceiver(receiver.get());
    }

    private void createDistributors(MatrixStore<Double> principalComponents, MatrixStore<Double> rotation)
    {
        for (val processor : getModel().getProcessors())
        {
            val protocol = getProtocol(processor.getId(), ProtocolType.DimensionReduction);
            if (!protocol.isPresent())
            {
                continue;
            }

            val distributor = createDistributor(protocol.get(), principalComponents, rotation);
            getModel().getDistributors().put(processor.getId(), distributor);
        }
    }

    private ActorRef createDistributor(Protocol receiverProtocol, MatrixStore<Double> principalComponents, MatrixStore<Double> rotation)
    {
        val model = DimensionReductionDistributorModel.builder()
                                                      .receiverProtocol(receiverProtocol)
                                                      .principalComponents(principalComponents)
                                                      .rotation(rotation)
                                                      .build();
        val control = new DimensionReductionDistributorControl(model);
        val distributor = trySpawnChild(ProtocolParticipant.props(control), "DimensionReductionDistributor");

        if (!distributor.isPresent())
        {
            getLog().error(String.format("Unable to create %1$s!", DimensionReductionDistributorControl.class.getName()));
            return ActorRef.noSender();
        }

        return distributor.get();
    }
}
