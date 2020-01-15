package de.hpi.msc.jschneider.math;

import lombok.Builder;
import lombok.Getter;

@Builder @Getter
public class Node
{
    private double intersectionLength;
    private double probability;
}