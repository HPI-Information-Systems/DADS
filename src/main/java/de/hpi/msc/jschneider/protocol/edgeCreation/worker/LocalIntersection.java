package de.hpi.msc.jschneider.protocol.edgeCreation.worker;

import de.hpi.msc.jschneider.math.Intersection;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

@SuperBuilder @Getter
public class LocalIntersection extends Intersection
{
    private int intersectionSegment;

    public static List<LocalIntersection> fromIntersectionCollection(IntersectionCollection intersectionCollection)
    {
        val localIntersections = new ArrayList<LocalIntersection>(intersectionCollection.getIntersections().size());
        for (val intersection : intersectionCollection.getIntersections())
        {
            localIntersections.add(LocalIntersection.builder()
                                                    .intersectionSegment(intersectionCollection.getIntersectionSegment())
                                                    .intersectionDistance(intersection.getIntersectionDistance())
                                                    .subSequenceIndex(intersection.getSubSequenceIndex())
                                                    .creationIndex(intersection.getCreationIndex())
                                                    .build());
        }

        return localIntersections;
    }
}
