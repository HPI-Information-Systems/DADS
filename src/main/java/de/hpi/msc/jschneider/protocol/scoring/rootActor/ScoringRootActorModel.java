package de.hpi.msc.jschneider.protocol.scoring.rootActor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class ScoringRootActorModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private ActorRef worker;
    @Setter @Getter
    private ActorRef receiver;
    @Getter
    private int queryLength;
    @Getter
    private int subSequenceLength;
}
