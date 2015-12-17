package io.paradoxical.cassandra.leadership;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.godaddy.logging.Logger;
import com.google.inject.Inject;
import io.paradoxical.cassandra.leadership.data.LeaderIdentity;
import io.paradoxical.cassandra.leadership.data.LeaderStatus;
import io.paradoxical.cassandra.leadership.data.LeadershipSchema;
import io.paradoxical.cassandra.leadership.interfaces.LeadershipStatus;
import io.paradoxical.cassandra.leadership.data.LeadershipGroup;

import java.util.List;
import java.util.stream.Collectors;

import static com.godaddy.logging.LoggerFactory.getLogger;

public class LeadershipStatusImpl implements LeadershipStatus {
    private static final Logger logger = getLogger(LeadershipStatusImpl.class);
    private final Session session;
    private final LeadershipSchema schema;

    @Inject
    public LeadershipStatusImpl(
            Session session,
            LeadershipSchema schema) {
        this.session = session;
        this.schema = schema;
    }


    @Override
    public List<LeaderStatus> listLeaders() {
        final Statement statement = QueryBuilder.select()
                                                .ttl(schema.getLeaderIdColumnName()).as("ttl")
                                                .column(schema.getGroupColumnName())
                                                .column(schema.getLeaderIdColumnName())
                                                .from(schema.getTableName())
                                                .setConsistencyLevel(ConsistencyLevel.SERIAL);

        return session.execute(statement)
                      .all()
                      .stream()
                      .map(this::map)
                      .collect(Collectors.toList());
    }

    protected LeaderStatus map(final Row row) {
        final String group = row.getString(schema.getGroupColumnName());

        final String leaderId = row.getString(schema.getLeaderIdColumnName());

        final int remainingTtl = row.getInt("ttl");

        return new LeaderStatus(LeadershipGroup.valueOf(group), LeaderIdentity.valueOf(leaderId), remainingTtl);
    }
}
