package de.hpi.msc.jschneider.protocol.statistics.rootActor;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

@SuperBuilder
public class StatisticsRootActorModel extends AbstractProtocolParticipantModel
{
    @Getter
    private StatisticsLog statisticsLog;
    @Setter @Getter
    private LocalDateTime calculationStartTime;
    @Setter @Getter
    private LocalDateTime calculationEndTime;
}
