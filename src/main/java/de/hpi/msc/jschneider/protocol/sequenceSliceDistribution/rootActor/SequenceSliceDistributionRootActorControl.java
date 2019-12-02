package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.rootActor;

import de.hpi.msc.jschneider.protocol.common.CommonMessages;
import de.hpi.msc.jschneider.protocol.common.control.AbstractProtocolParticipantControl;
import de.hpi.msc.jschneider.utility.ImprovedReceiveBuilder;

public class SequenceSliceDistributionRootActorControl extends AbstractProtocolParticipantControl<SequenceSliceDistributionRootActorModel>
{
    protected SequenceSliceDistributionRootActorControl(SequenceSliceDistributionRootActorModel model)
    {
        super(model);
    }

    @Override
    public ImprovedReceiveBuilder complementReceiveBuilder(ImprovedReceiveBuilder builder)
    {
        return builder.match(CommonMessages.SetUpProtocolMessage.class, this::onSetUp);
    }

    private void onSetUp(CommonMessages.SetUpProtocolMessage message)
    {

    }
}
