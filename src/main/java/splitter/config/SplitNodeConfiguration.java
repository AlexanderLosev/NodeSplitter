package splitter.config;

import java.util.*;

public class SplitNodeConfiguration {
    private static final String START_INDEX = "startIndex";
    private static final String INDEX_PROPERTY_NAME = "indexProperty";
    private static final String RELATIONSHIP_TYPES = "relationshipTypes";
    private static final String GREEDY_RELATIONSHIP_TYPES = "greedyRelationshipTypes";


    private final String indexPropertyName;
    private int startIndex;
    private HashSet<String> relationshipTypes;
    private HashSet<String> greedyRelationshipTypes;

    public static SplitNodeConfiguration build(Map<String,Object> configuration) {
        return new SplitNodeConfiguration(configuration);
    }

    private SplitNodeConfiguration(Map<String,Object> configuration) {
        Object indexPropertyName = configuration.get(INDEX_PROPERTY_NAME);
        this.indexPropertyName = indexPropertyName == null ? null : indexPropertyName.toString();

        parseStartIndex(configuration);
        relationshipTypes = parseRelationshipTypes(configuration, RELATIONSHIP_TYPES);
        greedyRelationshipTypes = parseRelationshipTypes(configuration, GREEDY_RELATIONSHIP_TYPES);
    }

    public String getIndexPropertyName() {
        return this.indexPropertyName;
    }

    public int getStartIndex() {
        return this.startIndex;
    }

    public Set<String> getRelationshipTypes() {
        return this.relationshipTypes;
    }

    public Set<String> getGreedyRelationshipTypes() {
        return this.greedyRelationshipTypes;
    }

    private void parseStartIndex(Map<String,Object> configuration) throws RuntimeException {
        Object startIndex = configuration.get(START_INDEX);
        if (startIndex != null) {
            try {
                this.startIndex = Integer.parseInt(startIndex.toString());
            }
            catch (NumberFormatException e)
            {
                throw new RuntimeException("Unable to parse " + START_INDEX + "value");
            }
        }
    }

    private HashSet<String> parseRelationshipTypes(Map<String,Object> configuration, String parameterName) throws RuntimeException {
        Object relationshipTypesValue = configuration.get(parameterName);
        if (relationshipTypesValue == null) {
            return new HashSet<>(0);
        }
        else if (relationshipTypesValue instanceof ArrayList) {
            ArrayList<String> values = (ArrayList<String>)relationshipTypesValue;
            return new HashSet<>(values);
        }
        else {
            throw new RuntimeException("Unable to parse " + parameterName + "value");
        }
    }
}
