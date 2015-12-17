package io.paradoxical.cassandra.leadership;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.godaddy.logging.Logger;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.paradoxical.cassandra.leadership.data.LeaderIdentity;
import io.paradoxical.cassandra.leadership.data.LeadershipGroup;
import io.paradoxical.cassandra.leadership.data.LeadershipSchema;
import io.paradoxical.cassandra.leadership.data.LeadershipToken;
import io.paradoxical.cassandra.leadership.interfaces.LeadershipElection;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.godaddy.logging.LoggerFactory.getLogger;

public class CassandraLeadershipElection implements LeadershipElection {
    private final Logger logger;

    private final Session session;
    private final LeadershipSchema schema;
    private final LeadershipGroup group;

    @Inject
    public CassandraLeadershipElection(
            Session session,
            LeadershipSchema schema,
            @Assisted LeadershipGroup group) {
        this.session = session;
        this.schema = schema;
        this.group = group;

        logger = getLogger(CassandraLeadershipElection.class).with("leadership-group", group);
    }

    @Override
    public Optional<LeadershipToken> tryClaimLeader(final LeaderIdentity identity, Duration timeToLive) {
        final Logger identityLogger = logger.with("identity", identity);

        final LeadershipToken tokenClaim = new LeadershipToken(timeToLive, identity, group);

        final Statement insert = QueryBuilder.insertInto(schema.getTableName())
                                             .ifNotExists()
                                             .value(schema.getGroupColumnName(), group.get())
                                             .value(schema.getLeaderIdColumnName(), identity.get())
                                             .using(ttl((int) timeToLive.getSeconds()));


        if (session.execute(insert).wasApplied()) {
            identityLogger.info("Leader determined");

            return Optional.of(tokenClaim);
        }

        final LeaderIdentity currentLeader = getLeader();

        if (currentLeader != null && currentLeader.equals(identity)) {
            identityLogger.with("current-leader", currentLeader)
                          .info("Re-entrant leader detected");

            return Optional.of(tokenClaim);
        }

        identityLogger.with("current-leader", currentLeader)
                      .trace("Other leader detected");

        return Optional.empty();
    }

    @Override
    public boolean tryHeartbeat(final LeadershipToken token) {
        final Statement update = QueryBuilder.update(schema.getTableName())
                                             .onlyIf(eq(schema.getLeaderIdColumnName(), token.getIdentity().get()))
                                             .with(set(schema.getLeaderIdColumnName(), token.getIdentity().get()))
                                             .using(ttl((int) token.getTtl().getSeconds()))
                                             .where(eq(schema.getGroupColumnName(), group.get()));

        return session.execute(update).wasApplied();
    }

    @Override
    public boolean renounceLeadership(final LeadershipToken claim) {
        final Statement update = QueryBuilder.delete()
                                             .all()
                                             .from(schema.getTableName())
                                             .onlyIf(eq(schema.getLeaderIdColumnName(), claim.getIdentity().get()))
                                             .where(eq(schema.getGroupColumnName(), group.get()));

        return session.execute(update).wasApplied();
    }

    @Override
    public LeaderIdentity getLeader() {
        final Statement statement = QueryBuilder.select()
                                                .column(schema.getLeaderIdColumnName())
                                                .from(schema.getTableName())
                                                .where(eq(schema.getGroupColumnName(), group.get()))
                                                .setConsistencyLevel(ConsistencyLevel.SERIAL);

        return getOne(session.execute(statement), row -> LeaderIdentity.valueOf(row.getString(0)));
    }

    private <T> T getOne(ResultSet resultSet, Function<Row, T> mapper) {
        Row row = resultSet.one();

        if (row == null) {
            return null;
        }

        return mapper.apply(row);
    }
}
