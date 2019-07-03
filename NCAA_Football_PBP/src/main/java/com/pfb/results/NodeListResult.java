package com.pfb.results;


import org.neo4j.graphdb.Node;

import java.util.List;

/**
 * @author mh
 * @since 26.02.16
 */
public class NodeListResult {
    public final List<Node> nodes;

    public NodeListResult(List<Node> value) {
        this.nodes = value;
    }
}