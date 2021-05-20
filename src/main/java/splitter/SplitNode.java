package splitter;

import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.procedure.*;
import splitter.config.SplitNodeConfiguration;
import splitter.results.SplitNodeResult;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SplitNode {
    @Context
    public GraphDatabaseService db;

    @Procedure(mode = Mode.WRITE)
    @Description("splitter.splitNodes([node1, node2]], {startIndex:0, indexProperty: 'Index', relationshipTypes: ['Type1', 'Type2'], greedyRelationTypes: ['Type3', 'Type4']}) Split each node from list into multiple nodes based on relationships with specified types")
    public Stream<SplitNodeResult> splitNodes(@Name("nodes") List<Node> nodes, @Name("configuration") Map<String,Object> configuration) {
        SplitNodeConfiguration config = SplitNodeConfiguration.build(configuration);
        return nodes.stream().flatMap(node -> splitNode(node, config).stream()).filter(Objects::nonNull).map(SplitNodeResult::new);
    }

    private List<Node> splitNode(Node node, SplitNodeConfiguration config) {
        if (node == null || node.getDegree() == 0)
            return Collections.emptyList();

        Transaction tx = db.beginTx();
        tx.acquireWriteLock(node);

        //collecting incoming relationships for which new nodes will not be created
        ArrayList<Relationship> ignoreIncomingRelationships = new ArrayList<>();
        node.getRelationships(Direction.INCOMING).forEach(relationship -> {
            tx.acquireWriteLock(relationship.getOtherNode(node));
            if (!config.getRelationshipTypes().contains(relationship.getType().name())
                && !config.getGreedyRelationshipTypes().contains(relationship.getType().name())) {
                ignoreIncomingRelationships.add(relationship);
            }
        });

        //collecting outgoing relationships for which new nodes will not be created
        ArrayList<Relationship> ignoreOutgoingRelationships = new ArrayList<>();
        node.getRelationships(Direction.OUTGOING).forEach(relationship -> {
            tx.acquireWriteLock(relationship.getOtherNode(node));
            if (!config.getRelationshipTypes().contains(relationship.getType().name())
                && !config.getGreedyRelationshipTypes().contains(relationship.getType().name())) {
                ignoreOutgoingRelationships.add(relationship);
            }
        });

        int index = config.getStartIndex();
        String indexProperty = config.getIndexPropertyName();
        //collections for created nodes
        List<Node> entrySplitNodes = new ArrayList<>();
        List<Node> exitSplitNodes = new ArrayList<>();

        for(String relationType: config.getRelationshipTypes()) {
            //for each "non-greedy" type of relationship create nodes and copy relationships separately
            index += createSplitNodesForRelationsipType(tx, node, relationType, indexProperty, index, new ArrayList<>(), entrySplitNodes, exitSplitNodes);
        }

        //collection for nodes which were created for outgoing "non-greedy" relationships
        //additional incoming "greedy" relationships will be created for this nodes
        List<Node> exitSplitNodesForNonGreedyRelationships = new ArrayList<>(exitSplitNodes);

        for(String relationType: config.getGreedyRelationshipTypes()) {
            //for each "greedy" type of relationship create nodes and copy relationships separately
            index += createSplitNodesForRelationsipType(tx, node, relationType, indexProperty, index, exitSplitNodesForNonGreedyRelationships, entrySplitNodes, exitSplitNodes);
        }

        //if no nodes were created return empty collection and leave source node alone
        if (entrySplitNodes.isEmpty() && exitSplitNodes.isEmpty())
            return Collections.emptyList();

        //copy relationships for which new nodes were not created for each created node
        repairRelationships(entrySplitNodes, exitSplitNodes, ignoreIncomingRelationships, ignoreOutgoingRelationships);

        //delete source node
        detachDeleteNode(node);
        return Stream.concat(entrySplitNodes.stream(), exitSplitNodes.stream()).collect(Collectors.toList());
    }

    /// returns count of created nodes
    private int createSplitNodesForRelationsipType(Transaction tx, Node node, String relationType, String indexProperty, int startIndex, List<Node> nonGreedyExitNodes, List<Node> resultEntryNodes, List<Node> resultExitNodes) {
        //collect source node relationships of target type
        List<Relationship> incomingRelationships = getRelationsips(tx, node, Direction.INCOMING, relationType);
        List<Relationship> outgoingRelationships = getRelationsips(tx, node, Direction.OUTGOING, relationType);

        //if incoming relationships not found or no and will not be nodes with outgoing relationships
        //then exit
        if (incomingRelationships.isEmpty() || (outgoingRelationships.isEmpty() && nonGreedyExitNodes.isEmpty()))
            return 0;

        int index = startIndex;
        //create separate node for each incoming relationship of target type
        List<Node> enterNodes = createSplitNodesForRelationsips(node, incomingRelationships, indexProperty, index, Direction.INCOMING);
        index += enterNodes.size();
        //create separate node for each outgoing relationship of target type
        List<Node> exitNodes = createSplitNodesForRelationsips(node, outgoingRelationships, indexProperty, index, Direction.OUTGOING);

        //copy relationships
        for (Node enterNode : enterNodes) {
            Relationship relationship = Iterables.first(enterNode.getRelationships());
            for (Node exitNode : exitNodes) {
                //copy relationship and connect incoming and outgoing nodes
                createRelationship(enterNode, exitNode, relationship);
            }
            //if nonGreedyExitNodes is not empty then this relationship type is "greedy"
            //so connect "greedy" entry node to "non-greedy" exit node
            for (Node exitNode : nonGreedyExitNodes) {
                //copy relationship and connect incoming and outgoing "non-greedy" nodes
                createRelationship(enterNode, exitNode, relationship);
            }
        }
        //copy created nodes to result collections
        resultEntryNodes.addAll(enterNodes);
        resultExitNodes.addAll(exitNodes);
        return enterNodes.size() + exitNodes.size();
    }

    private List<Relationship> getRelationsips(Transaction tx, Node node, Direction direction, String relationType) {
        ArrayList<Relationship> result = new ArrayList<>();
        node.getRelationships(direction).forEach(relationship -> {
            tx.acquireWriteLock(relationship.getOtherNode(node));
            if (relationship.getType().name().equals(relationType)) {
                result.add(relationship);
            }
        });
        return result;
    }

    private void createRelationship(Node from, Node to, Relationship source) {
        Relationship relationship = from.createRelationshipTo(to, source.getType());
        source.getAllProperties().forEach(relationship::setProperty);
    }

    private void detachDeleteNode(Node node) {
        node.getRelationships().forEach(Relationship::delete);
        node.delete();
    }

    private Relationship createRelationship(Node targetNode, Node otherNode, Direction direction, RelationshipType type) {
        if (direction == Direction.INCOMING) {
            return otherNode.createRelationshipTo(targetNode, type);
        } else {
            return targetNode.createRelationshipTo(otherNode, type);
        }
    }

    private List<Node> createSplitNodesForRelationsips(Node source, List<Relationship> relationships, String indexProperty, int startIndex, Direction direction) {
        ArrayList<Node> splitNodes = new ArrayList<>(relationships.size());
        for (Relationship relationship : relationships) {
            Node splitNode = createSplitNode(source, indexProperty, startIndex);
            if (direction == Direction.INCOMING) {
                Node otherNode = relationship.getStartNode();
                createRelationship(otherNode, splitNode, relationship);
            } else {
                Node otherNode = relationship.getEndNode();
                createRelationship(splitNode, otherNode, relationship);
            }
            startIndex++;
            splitNodes.add(splitNode);
        }
        return splitNodes;
    }

    private Node createSplitNode(Node source, String indexPropertyName, int index) {
        Label[] labels = Iterables.asArray(Label.class, source.getLabels());
        Node node = db.createNode(labels);
        if (indexPropertyName != null) { node.setProperty(indexPropertyName, index); }
        source.getAllProperties().forEach(node::setProperty);
        return node;
    }

    private void repairRelationships(List<Node> enterNodes, List<Node> exitNodes, List<Relationship> incoming, List<Relationship> outgoing) {
        enterNodes.forEach(enterNode -> repairRelationships(enterNode, incoming, Direction.INCOMING));
        enterNodes.forEach(enterNode -> repairRelationships(enterNode, outgoing, Direction.OUTGOING));
        exitNodes.forEach(exitNode -> repairRelationships(exitNode, incoming, Direction.INCOMING));
        exitNodes.forEach(exitNode -> repairRelationships(exitNode, outgoing, Direction.OUTGOING));
    }

    private void repairRelationships(Node node, List<Relationship> relationships, Direction direction) {
        if (direction == Direction.BOTH) return;
        relationships.forEach(relationship -> {
            Node otherNode = direction == Direction.INCOMING ? relationship.getStartNode() : relationship.getEndNode();
            Relationship repairedRelationship = createRelationship(node, otherNode, direction, relationship.getType());
            relationship.getAllProperties().forEach(repairedRelationship::setProperty);
        });
    }
}
