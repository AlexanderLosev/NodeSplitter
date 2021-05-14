package splitter;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SplitNodeTest {
    private static final Config driverConfig = Config.build().withoutEncryption().toConfig();
    private ServerControls embeddedDatabaseServer;

    @BeforeAll
    void initializeNeo4j() {

        this.embeddedDatabaseServer = TestServerBuilders
                .newInProcessBuilder()
                .withProcedure(SplitNode.class)
                .newServer();
    }

    @Test
    public void splitNodeTest() {
        try(Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
            Session session = driver.session()){
            session.run("CREATE (n1:A) SET n1.TestId = 1\n" +
                    "        CREATE (n2:A) SET n2.TestId = 2\n" +
                    "        CREATE (n3:A) SET n3.TestId = 3\n" +
                    "        CREATE (m:B)\n" +
                    "        CREATE (n2)-[r1:Rel]->(n1) SET r1.TestId = 101\n" +
                    "        CREATE (n3)-[r2:Rel]->(n1) SET r2.TestId = 102\n" +
                    "        CREATE (n1)-[r3:Rel]->(n2) SET r3.TestId = 201\n" +
                    "        CREATE (n1)-[r4:Rel]->(n3) SET r4.TestId = 202\n" +
                    "        CREATE (b:B)\n" +
                    "        CREATE (n1)-[r5:OtherRel]->(b) SET r5.TestId = 301");

            session.run("MATCH (n:A) where n.TestId = 1 WITH collect(n) as nodes CALL splitter.splitNodes(nodes, {startIndex: 0, indexProperty: \"SplitId\", relationshipTypes: [\"Rel\", \"SecondRel\"]}) YIELD node RETURN node");

            List<Record> records = session.run("MATCH (n1:A) WHERE n1.TestId = 1 return n1").list();
            assertEquals(4, records.size());
        }
    }

    @Test
    public void splitNodeTwoRelationsTest() {
        try(Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
            Session session = driver.session()){
            session.run("CREATE (n1:A) SET n1.TestId = 1\n" +
                    "        CREATE (n2:A) SET n2.TestId = 2\n" +
                    "        CREATE (n3:A) SET n3.TestId = 3\n" +
                    "        CREATE (n4:A) SET n4.TestId = 4\n" +
                    "        CREATE (n1)-[r1:Rel]->(n2) SET r1.TestId = 102\n" +
                    "        CREATE (n2)-[r2:Rel]->(n3) SET r2.TestId = 203\n" +
                    "        CREATE (n2)-[r3:Rel]->(n4) SET r3.TestId = 204\n" +
                    "        CREATE (n4)-[r4:Rel]->(n2) SET r4.TestId = 402\n" +
                    "        CREATE (n2)-[r5:OtherRel]->(n1) SET r5.TestId = 201\n" +
                    "        CREATE (n3)-[r6:OtherRel]->(n2) SET r6.TestId = 302");

            session.run("MATCH (n:A) where n.TestId = 2 WITH collect(n) as nodes CALL splitter.splitNodes(nodes, {startIndex: 0, indexProperty: \"SplitId\", relationshipTypes: [\"Rel\", \"OtherRel\"]}) YIELD node RETURN node");

            List<Record> incomRelRecords = session.run("MATCH ()-[r:Rel]->(n:A) where n.TestId = 2 return r").list();
            // incoming relations are: 1->2(1), 2(1) -> 2(3), 2(1)->2(4-o), 4->2(4-i), 2(4-i)->2(4-o), 2(4-i)->2(3)
            assertEquals(6, incomRelRecords.size());

            incomRelRecords = session.run("MATCH ()-[r:Rel]->(n:A) where n.TestId = 2 return distinct n").list();
            // nodes with incoming relations are: 2(1), 2(3), 2(4-o), 2(4-i)
            assertEquals(4, incomRelRecords.size());

            List<Record> outgoingRelRecords = session.run("MATCH (n:A)-[r:Rel]->() where n.TestId = 2 return r").list();
            // outgoing relations are: 2(1)->2(4-o), 2(1)->2(3), 2(3)->3, 2(4-o)->4, 2(4-i)->2(3), 2(4-o)->2(4-i)
            assertEquals(6, outgoingRelRecords.size());

            outgoingRelRecords = session.run("MATCH (n:A)-[r:Rel]->() where n.TestId = 2 return distinct n").list();
            // nodes with outgoing relations are: 2(1), 2(3), 2(4-o), 2(4-i)
            assertEquals(4, outgoingRelRecords.size());

            List<Record> incomOtherRelRecords = session.run("MATCH ()-[:OtherRel]->(n:A) where n.TestId = 2 return n").list();
            // incoming other relations are: 3->2(3), 2(3) -> 2(1)
            assertEquals(2,incomOtherRelRecords.size());

            List<Record> outgoingOtherRelRecords = session.run("MATCH (n:A)-[:OtherRel]->() where n.TestId = 2 return n").list();
            // outgoing other relations are: 2(3)->2(1), 2(1) -> 1
            assertEquals(2, incomOtherRelRecords.size());
        }
    }
}
