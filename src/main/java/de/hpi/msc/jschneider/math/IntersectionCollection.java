package de.hpi.msc.jschneider.math;

import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Builder @Getter
public class IntersectionCollection
{
    private int intersectionPointIndex;
    private final List<Intersection> intersections = new ArrayList<>();
}
