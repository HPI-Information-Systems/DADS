package de.hpi.msc.jschneider.math;

import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import it.unimi.dsi.fastutil.objects.ObjectBigList;
import lombok.Getter;

@Getter
public class IntersectionCollection
{
    private final int intersectionSegment;
    private final ObjectBigList<Intersection> intersections;

    public IntersectionCollection(int intersectionSegment, long estimatedNumberOfIntersections)
    {
        this.intersectionSegment = intersectionSegment;
        intersections = new ObjectBigArrayBigList<>(Math.max(estimatedNumberOfIntersections, ObjectBigArrayBigList.DEFAULT_INITIAL_CAPACITY));
    }
}
