package io.paradoxical.cassandra.leadership.interfaces;

import io.paradoxical.cassandra.leadership.data.LeadershipGroup;

public interface LeadershipElectionFactory {
    LeadershipElection create(LeadershipGroup key);
}

