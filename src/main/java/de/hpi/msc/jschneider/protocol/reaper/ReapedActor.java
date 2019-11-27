package de.hpi.msc.jschneider.protocol.reaper;

import akka.actor.Props;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeParticipantControl;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeParticipantModel;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeProtocolParticipant;

public class ReapedActor<TModel extends MessageExchangeParticipantModel, TControl extends MessageExchangeParticipantControl<TModel>> extends MessageExchangeProtocolParticipant<TModel, TControl>
{
    public static <TModel extends MessageExchangeParticipantModel, TControl extends MessageExchangeParticipantControl<TModel>> Props props(TControl control)
    {
        return Props.create(ReapedActor.class, () -> new ReapedActor(control));
    }

    protected ReapedActor(TControl control)
    {
        super(control);
    }

    @Override
    public void preStart() throws Exception
    {
        super.preStart();

        if (!ReaperProtocol.isInitialized())
        {
            return;
        }

        getControl().send(ReaperMessages.WatchMeMessage.builder()
                                                       .sender(getSelf())
                                                       .receiver(ReaperProtocol.getLocalRootActor())
                                                       .build());
    }
}
