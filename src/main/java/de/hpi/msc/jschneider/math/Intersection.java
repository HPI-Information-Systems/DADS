package de.hpi.msc.jschneider.math;

import lombok.Builder;
import lombok.Getter;

@Builder @Getter
public class Intersection
{
    private float intersectionDistance;
    private long subSequenceIndex;
}
