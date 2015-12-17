package io.paradoxical.cassandra.leadership.interfaces;

import io.paradoxical.cassandra.leadership.data.LeaderStatus;

import java.util.List;

public interface LeadershipStatus {
    List<LeaderStatus> listLeaders();
}
