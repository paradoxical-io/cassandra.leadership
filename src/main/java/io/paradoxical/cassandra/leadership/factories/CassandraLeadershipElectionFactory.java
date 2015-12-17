package io.paradoxical.cassandra.leadership.factories;

import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import io.paradoxical.cassandra.leadership.CassandraLeadershipElection;
import io.paradoxical.cassandra.leadership.data.LeadershipSchema;
import io.paradoxical.cassandra.leadership.interfaces.LeadershipElectionFactory;
import io.paradoxical.cassandra.leadership.data.LeadershipGroup;
import io.paradoxical.cassandra.leadership.interfaces.LeadershipElection;

public class CassandraLeadershipElectionFactory implements LeadershipElectionFactory {
    private final Session session;

    @Inject
    public CassandraLeadershipElectionFactory(Session session) {
        this.session = session;
    }

    @Override
    public LeadershipElection create(final LeadershipGroup key) {
        return new CassandraLeadershipElection(session, LeadershipSchema.Default, key);
    }
}
