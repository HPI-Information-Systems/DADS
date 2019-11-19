package de.hpi.msc.jschneider.actor.common.reaper;

import lombok.NoArgsConstructor;

import java.io.Serializable;

public class ReaperMessages
{
    @NoArgsConstructor
    public static class WatchMeMessage implements Serializable
    {
        private static final long serialVersionUID = 159612977766147061L;
    }
}
