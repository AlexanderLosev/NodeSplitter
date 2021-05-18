# NodeSplitter
Plugin for neo4j for splitting a single node into multiple nodes

Usage: `splitter.splitNodes([node1, node2]], {startIndex:0, indexProperty: 'Index', relationshipTypes: ['Type1', 'Type2'], greedyRelationshipTypes: ['Type3', 'Type4']})`

### Arguments
1. List of nodes to split
2. Parameters

### Parameters
|Parameter        |Description                                                                |Required                                           |
|-----------------|---------------------------------------------------------------------------|---------------------------------------------------|
|indexProperty    |Name of property that will be added to all result nodes and used as indexer|No                                                 |
|startIndex       |First index                                                                |Yes, will not be used if `indexProperty` is not set|
|relationshipTypes|List of relationship types that will be used for splitting. For each incoming and outgoing relationship new node will be created and each "incoming" node will be linked to each "outgoing" node                 |Yes                                                |
|greedyRelationshipTypes|List of greedy relationship types that will be used for splitting. After splitting each new node with incoming greedy relationship will be linked to all "outgoing" nodes|No|