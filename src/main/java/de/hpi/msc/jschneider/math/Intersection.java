package de.hpi.msc.jschneider.math;

import lombok.Getter;
import lombok.experimental.SuperBuilder;

@SuperBuilder @Getter
public class Intersection
{
    private double intersectionDistance;
    private long subSequenceIndex;
}
