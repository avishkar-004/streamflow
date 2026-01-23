package com.streamflow.broker.replication;

import com.streamflow.broker.storage.Partition;
import com.streamflow.common.model.Message;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fetches data from leader to follower replica
 *
 * Runs in background thread for each follower partition.
 * Continuously fetches new messages from leader and appends locally.
 *
 * Flow:
 * 1. Check current local offset
 * 2. Fetch messages from leader starting at that offset
 * 3. Append messages to local partition
 * 4. Update offset
 * 5. Repeat
 */
@Slf4j
public class ReplicaFetcher implements Runnable {

    private final String topicName;
    private final int partitionId;
    private final Partition localPartition;
    private final ReplicaManager replicaManager;  // To fetch from leader
    private final int leaderBrokerId;

    private final AtomicBoolean running;
    private long fetchOffset;

    // Fetch parameters
    private static final int MAX_FETCH_MESSAGES = 100;
    private static final int MAX_FETCH_BYTES = 1024 * 1024; // 1MB
    private static final long FETCH_INTERVAL_MS = 500; // Fetch every 500ms

    public ReplicaFetcher(String topicName, int partitionId, Partition localPartition,
                         ReplicaManager replicaManager, int leaderBrokerId) {
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.localPartition = localPartition;
        this.replicaManager = replicaManager;
        this.leaderBrokerId = leaderBrokerId;
        this.running = new AtomicBoolean(true);
        this.fetchOffset = localPartition.getLogEndOffset();
    }

    @Override
    public void run() {
        log.info("Started replica fetcher for {}-{} from broker {}",
                topicName, partitionId, leaderBrokerId);

        while (running.get()) {
            try {
                // Fetch messages from leader
                List<Message> messages = fetchFromLeader();

                if (messages != null && !messages.isEmpty()) {
                    // Append to local partition
                    localPartition.appendBatch(messages);

                    // Update fetch offset
                    fetchOffset = localPartition.getLogEndOffset();

                    log.debug("Fetched {} messages for {}-{}, new offset: {}",
                            messages.size(), topicName, partitionId, fetchOffset);
                } else {
                    // No new messages, sleep before retry
                    Thread.sleep(FETCH_INTERVAL_MS);
                }

            } catch (InterruptedException e) {
                log.info("Replica fetcher interrupted for {}-{}", topicName, partitionId);
                break;
            } catch (Exception e) {
                log.error("Error fetching from leader for {}-{}", topicName, partitionId, e);
                try {
                    Thread.sleep(FETCH_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    break;
                }
            }
        }

        log.info("Stopped replica fetcher for {}-{}", topicName, partitionId);
    }

    /**
     * Fetch messages from leader
     * In real implementation, this would make network call to leader broker
     * For simplicity, we fetch from local replica manager
     */
    private List<Message> fetchFromLeader() {
        // In a real distributed system, this would be a network call to leader broker
        // For now, we simulate by fetching from the replica manager
        return replicaManager.fetchMessages(topicName, partitionId, fetchOffset,
                MAX_FETCH_MESSAGES, MAX_FETCH_BYTES);
    }

    /**
     * Stop the fetcher
     */
    public void stop() {
        running.set(false);
        log.info("Stopping replica fetcher for {}-{}", topicName, partitionId);
    }

    /**
     * Check if fetcher is running
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Get current fetch offset
     */

    public void shutdown() {
        running = false;
        interrupt();
    }

    public int getLeaderBrokerId() { return leaderBrokerId; }
}
