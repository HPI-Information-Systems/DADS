package de.hpi.msc.jschneider.data.graph;

import de.hpi.msc.jschneider.utility.Counter;
import junit.framework.TestCase;
import lombok.val;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGraphEdge extends TestCase
{
    public void testHashDependsOnDirection()
    {
        val nodeA = GraphNode.builder()
                             .intersectionSegment(0)
                             .index(1)
                             .build();
        val nodeB = GraphNode.builder()
                             .intersectionSegment(1)
                             .index(0)
                             .build();

        val edgeAToB = GraphEdge.builder()
                                .from(nodeA)
                                .to(nodeB)
                                .weight(new Counter(1L))
                                .build();
        val edgeBToA = GraphEdge.builder()
                                .from(nodeB)
                                .to(nodeA)
                                .weight(new Counter(1L))
                                .build();

        assertThat(edgeAToB.hashCode()).isNotEqualTo(edgeBToA.hashCode());
    }

    public void testHashDoesNotDependOnWeight()
    {
        val nodeA = GraphNode.builder()
                             .intersectionSegment(0)
                             .index(1)
                             .build();
        val nodeB = GraphNode.builder()
                             .intersectionSegment(1)
                             .index(0)
                             .build();

        val edge1 = GraphEdge.builder()
                                .from(nodeA)
                                .to(nodeB)
                                .weight(new Counter(1L))
                                .build();

        val edge2 = GraphEdge.builder()
                             .from(nodeA)
                             .to(nodeB)
                             .weight(new Counter(3L))
                             .build();

        assertThat(edge1.hashCode()).isEqualTo(edge2.hashCode());
    }
}
