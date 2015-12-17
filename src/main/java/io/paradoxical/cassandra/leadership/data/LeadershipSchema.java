package io.paradoxical.cassandra.leadership.data;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class LeadershipSchema {
    private final String tableName;
    private final String groupColumnName;
    private final String leaderIdColumnName;

    public static LeadershipSchema Default =
            LeadershipSchema.builder()
                            .tableName("leadership_election")
                            .groupColumnName("group")
                            .leaderIdColumnName("leader_id")
                            .build();
}
