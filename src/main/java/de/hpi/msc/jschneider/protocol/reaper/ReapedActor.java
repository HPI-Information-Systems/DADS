package de.hpi.msc.jschneider.protocol.reaper;

import akka.actor.Props;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
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
        getControl().getLocalProtocol(ProtocolType.Reaper).ifPresent(protocol ->
                                                                     {
                                                                         getControl().send(ReaperMessages.WatchMeMessage.builder()
                                                                                                                        .sender(getSelf())
                                                                                                                        .receiver(protocol.getRootActor())
                                                                                                                        .build());
                                                                     });
    }
}
