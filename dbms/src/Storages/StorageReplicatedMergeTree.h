#pragma once

#include <ext/shared_ptr_helper.h>
#include <atomic>
#include <pcg_random.hpp>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMerger.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/MergeTree/ReplicatedMergeTreeCleanupThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreeRestartingThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartCheckThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAlterThread.h>
#include <Storages/MergeTree/AbandonableLockInZooKeeper.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/MergeTree/DataPartsExchange.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/randomSeed.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/LeaderElection.h>


namespace DB
{

/** The engine that uses the merge tree (see MergeTreeData) and replicated through ZooKeeper.
  *
  * ZooKeeper is used for the following things:
  * - the structure of the table (/ metadata, /columns)
  * - action log with data (/log/log-...,/replicas/replica_name/queue/queue-...);
  * - a replica list (/replicas), and replica activity tag (/replicas/replica_name/is_active), replica addresses (/replicas/replica_name/host);
  * - select the leader replica (/leader_election) - this is the replica that assigns the merge;
  * - a set of parts of data on each replica (/replicas/replica_name/parts);
  * - list of the last N blocks of data with checksum, for deduplication (/blocks);
  * - the list of incremental block numbers (/block_numbers) that we are about to insert,
  *   to ensure the linear order of data insertion and data merge only on the intervals in this sequence;
  * - coordinates writes with quorum (/quorum).
  */

/** The replicated tables have a common log (/log/log-...).
  * Log - a sequence of entries (LogEntry) about what to do.
  * Each entry is one of:
  * - normal data insertion (GET),
  * - merge (MERGE),
  * - delete the partition (DROP).
  *
  * Each replica copies (queueUpdatingThread, pullLogsToQueue) entries from the log to its queue (/replicas/replica_name/queue/queue-...)
  *  and then executes them (queueTask).
  * Despite the name of the "queue", execution can be reordered, if necessary (shouldExecuteLogEntry, executeLogEntry).
  * In addition, the records in the queue can be generated independently (not from the log), in the following cases:
  * - when creating a new replica, actions are put on GET from other replicas (createReplica);
  * - if the part is corrupt (removePartAndEnqueueFetch) or absent during the check (at start - checkParts, while running - searchForMissingPart),
  *   actions are put on GET from other replicas;
  *
  * The replica to which INSERT was made in the queue will also have an entry of the GET of this data.
  * Such an entry is considered to be executed as soon as the queue handler sees it.
  *
  * The log entry has a creation time. This time is generated by the clock of server that created entry
  * - the one on which the corresponding INSERT or ALTER query came.
  *
  * For the entries in the queue that the replica made for itself,
  * as the time will take the time of creation the appropriate part on any of the replicas.
  */

class StorageReplicatedMergeTree : public ext::shared_ptr_helper<StorageReplicatedMergeTree>, public IStorage
{
public:
    void startup() override;
    void shutdown() override;
    ~StorageReplicatedMergeTree() override;

    std::string getName() const override
    {
        return "Replicated" + data.merging_params.getModeName() + "MergeTree";
    }

    std::string getTableName() const override { return table_name; }
    bool supportsSampling() const override { return data.supportsSampling(); }
    bool supportsFinal() const override { return data.supportsFinal(); }
    bool supportsPrewhere() const override { return data.supportsPrewhere(); }
    bool supportsReplication() const override { return true; }

    const ColumnsDescription & getColumns() const override { return data.getColumns(); }
    void setColumns(ColumnsDescription columns_) override { return data.setColumns(std::move(columns_)); }

    NameAndTypePair getColumn(const String & column_name) const override
    {
        return data.getColumn(column_name);
    }

    bool hasColumn(const String & column_name) const override
    {
        return data.hasColumn(column_name);
    }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    bool optimize(const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context) override;

    void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

    void clearColumnInPartition(const ASTPtr & partition, const Field & column_name, const Context & context) override;
    void dropPartition(const ASTPtr & query, const ASTPtr & partition, bool detach, const Context & context) override;
    void attachPartition(const ASTPtr & partition, bool part, const Context & context) override;
    void fetchPartition(const ASTPtr & partition, const String & from, const Context & context) override;
    void freezePartition(const ASTPtr & partition, const String & with_name, const Context & context) override;

    void mutate(const MutationCommands & commands, const Context & context) override;

    /** Removes a replica from ZooKeeper. If there are no other replicas, it deletes the entire table from ZooKeeper.
      */
    void drop() override;

    void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(const ASTPtr & left_in_operand) const override { return data.mayBenefitFromIndexForIn(left_in_operand); }

    bool checkTableCanBeDropped() const override;

    MergeTreeData & getData() { return data; }
    const MergeTreeData & getData() const { return data; }


    /** For the system table replicas. */
    struct Status
    {
        bool is_leader;
        bool is_readonly;
        bool is_session_expired;
        ReplicatedMergeTreeQueue::Status queue;
        UInt32 parts_to_check;
        String zookeeper_path;
        String replica_name;
        String replica_path;
        Int32 columns_version;
        UInt64 log_max_index;
        UInt64 log_pointer;
        UInt64 absolute_delay;
        UInt8 total_replicas;
        UInt8 active_replicas;
    };

    /// Get the status of the table. If with_zk_fields = false - do not fill in the fields that require queries to ZK.
    void getStatus(Status & res, bool with_zk_fields = true);

    using LogEntriesData = std::vector<ReplicatedMergeTreeLogEntryData>;
    void getQueue(LogEntriesData & res, String & replica_name);

    /// Get replica delay relative to current time.
    time_t getAbsoluteDelay() const;

    /// If the absolute delay is greater than min_relative_delay_to_yield_leadership,
    /// will also calculate the difference from the unprocessed time of the best replica.
    /// NOTE: Will communicate to ZooKeeper to calculate relative delay.
    void getReplicaDelays(time_t & out_absolute_delay, time_t & out_relative_delay);

    /// Add a part to the queue of parts whose data you want to check in the background thread.
    void enqueuePartForCheck(const String & part_name, time_t delay_to_check_seconds = 0)
    {
        part_check_thread.enqueuePart(part_name, delay_to_check_seconds);
    }

    String getDataPath() const override { return full_path; }

private:
    /// Delete old parts from disk and from ZooKeeper.
    void clearOldPartsAndRemoveFromZK();

    friend class ReplicatedMergeTreeBlockOutputStream;
    friend class ReplicatedMergeTreeRestartingThread;
    friend class ReplicatedMergeTreePartCheckThread;
    friend class ReplicatedMergeTreeCleanupThread;
    friend class ReplicatedMergeTreeAlterThread;
    friend class ReplicatedMergeTreeRestartingThread;
    friend struct ReplicatedMergeTreeLogEntry;
    friend class ScopedPartitionMergeLock;

    using LogEntry = ReplicatedMergeTreeLogEntry;
    using LogEntryPtr = LogEntry::Ptr;

    Context & context;

    zkutil::ZooKeeperPtr current_zookeeper;        /// Use only the methods below.
    std::mutex current_zookeeper_mutex;            /// To recreate the session in the background thread.

    zkutil::ZooKeeperPtr tryGetZooKeeper();
    zkutil::ZooKeeperPtr getZooKeeper();
    void setZooKeeper(zkutil::ZooKeeperPtr zookeeper);

    /// If true, the table is offline and can not be written to it.
    std::atomic_bool is_readonly {false};

    String database_name;
    String table_name;
    String full_path;

    String zookeeper_path;
    String replica_name;
    String replica_path;

    /** /replicas/me/is_active.
      */
    zkutil::EphemeralNodeHolderPtr replica_is_active_node;

    /** Version node /columns in ZooKeeper corresponding to the current data.columns.
      * Read and modify along with the data.columns - under TableStructureLock.
      */
    int columns_version = -1;

    /** Is this replica "leading". The leader replica selects the parts to merge.
      */
    std::atomic<bool> is_leader {false};
    zkutil::LeaderElectionPtr leader_election;

    InterserverIOEndpointHolderPtr data_parts_exchange_endpoint_holder;

    MergeTreeData data;
    MergeTreeDataSelectExecutor reader;
    MergeTreeDataWriter writer;
    MergeTreeDataMerger merger;

    /** The queue of what needs to be done on this replica to catch up with everyone. It is taken from ZooKeeper (/replicas/me/queue/).
     * In ZK entries in chronological order. Here it is not necessary.
     */
    ReplicatedMergeTreeQueue queue;
    std::atomic<time_t> last_queue_update_start_time{0};
    std::atomic<time_t> last_queue_update_finish_time{0};

    DataPartsExchange::Fetcher fetcher;


    /// When activated, replica is initialized and startup() method could exit
    Poco::Event startup_event;

    /// Do I need to complete background threads (except restarting_thread)?
    std::atomic<bool> shutdown_called {false};
    Poco::Event shutdown_event;

    /// Limiting parallel fetches per one table
    std::atomic_uint current_table_fetches {0};

    /// Threads.

    /// A thread that keeps track of the updates in the logs of all replicas and loads them into the queue.
    std::thread queue_updating_thread;
    zkutil::EventPtr queue_updating_event = std::make_shared<Poco::Event>();

    std::thread mutations_updating_thread;
    zkutil::EventPtr mutations_updating_event = std::make_shared<Poco::Event>();

    /// A task that performs actions from the queue.
    BackgroundProcessingPool::TaskHandle queue_task_handle;

    /// A thread that selects parts to merge.
    std::thread merge_selecting_thread;
    Poco::Event merge_selecting_event;
    /// It is acquired for each iteration of the selection of parts to merge or each OPTIMIZE query.
    std::mutex merge_selecting_mutex;
    /// If true then new entries might added to the queue, so we must pull logs before selecting parts for merge.
    /// Is used only to avoid superfluous pullLogsToQueue() calls
    bool merge_selecting_logs_pulling_is_required = true;

    /// A thread that removes old parts, log entries, and blocks.
    std::unique_ptr<ReplicatedMergeTreeCleanupThread> cleanup_thread;
    /// Is used to wakeup cleanup_thread
    Poco::Event cleanup_thread_event;

    /// A thread that processes reconnection to ZooKeeper when the session expires.
    std::unique_ptr<ReplicatedMergeTreeRestartingThread> restarting_thread;

    /// A thread monitoring changes to the column list in ZooKeeper and updating the parts in accordance with these changes.
    std::unique_ptr<ReplicatedMergeTreeAlterThread> alter_thread;

    /// A thread that checks the data of the parts, as well as the queue of the parts to be checked.
    ReplicatedMergeTreePartCheckThread part_check_thread;

    /// An event that awakens `alter` method from waiting for the completion of the ALTER query.
    zkutil::EventPtr alter_query_event = std::make_shared<Poco::Event>();

    Logger * log;

    /// Initialization.

    /** Creates the minimum set of nodes in ZooKeeper.
      */
    void createTableIfNotExists();

    /** Creates a replica in ZooKeeper and adds to the queue all that it takes to catch up with the rest of the replicas.
      */
    void createReplica();

    /** Create nodes in the ZK, which must always be, but which might not exist when older versions of the server are running.
      */
    void createNewZooKeeperNodes();

    /** Verify that the list of columns and table settings match those specified in ZK (/metadata).
      * If not, throw an exception.
      */
    void checkTableStructure(bool skip_sanity_checks, bool allow_alter);

    /** Check that the set of parts corresponds to that in ZK (/replicas/me/parts/).
      * If any parts described in ZK are not locally, throw an exception.
      * If any local parts are not mentioned in ZK, remove them.
      *  But if there are too many, throw an exception just in case - it's probably a configuration error.
      */
    void checkParts(bool skip_sanity_checks);

    /** Check that the part's checksum is the same as the checksum of the same part on some other replica.
      * If no one has such a part, nothing checks.
      * Not very reliable: if two replicas add a part almost at the same time, no checks will occur.
      * Adds actions to `ops` that add data about the part into ZooKeeper.
      * Call under TableStructureLock.
      */
    void checkPartChecksumsAndAddCommitOps(const zkutil::ZooKeeperPtr & zookeeper, const MergeTreeData::DataPartPtr & part,
                                           zkutil::Requests & ops, String part_name = "", NameSet * absent_replicas_paths = nullptr);

    String getChecksumsForZooKeeper(const MergeTreeDataPartChecksums & checksums);

    /// Accepts a PreComitted part, atomically checks its checksums with ones on other replicas and commit the part
    MergeTreeData::DataPartsVector checkPartChecksumsAndCommit(MergeTreeData::Transaction & transaction,
                                                               const MergeTreeData::DataPartPtr & part);

    /// Adds actions to `ops` that remove a part from ZooKeeper.
    void removePartFromZooKeeper(const String & part_name, zkutil::Requests & ops);

    /// Quickly removes big set of parts from ZooKeeper (using async multi queries)
    void removePartsFromZooKeeper(zkutil::ZooKeeperPtr & zookeeper, const Strings & part_names,
                                  NameSet * parts_should_be_retried = nullptr);

    /// Removes a part from ZooKeeper and adds a task to the queue to download it. It is supposed to do this with broken parts.
    void removePartAndEnqueueFetch(const String & part_name);

    /// Running jobs from the queue.

    /** Copies the new entries from the logs of all replicas to the queue of this replica.
      * If next_update_event != nullptr, calls this event when new entries appear in the log.
      */
    void pullLogsToQueue(zkutil::EventPtr next_update_event);

    /** Execute the action from the queue. Throws an exception if something is wrong.
      * Returns whether or not it succeeds. If it did not work, write it to the end of the queue.
      */
    bool executeLogEntry(const LogEntry & entry);

    void executeDropRange(const LogEntry & entry);

    /// Do the merge or recommend to make the fetch instead of the merge
    void tryExecuteMerge(const LogEntry & entry, bool & do_fetch);

    bool executeFetch(const LogEntry & entry);

    void executeClearColumnInPartition(const LogEntry & entry);

    /** Updates the queue.
      */
    void queueUpdatingThread();

    void mutationsUpdatingThread();

    /** Performs actions from the queue.
      */
    bool queueTask();

    /// Postcondition:
    /// either leader_election is fully initialized (node in ZK is created and the watching thread is launched)
    /// or an exception is thrown and leader_election is destroyed.
    void enterLeaderElection();

    /// Postcondition:
    /// is_leader is false, merge_selecting_thread is stopped, leader_election is nullptr.
    /// leader_election node in ZK is either deleted, or the session is marked expired.
    void exitLeaderElection();

    /** Selects the parts to merge and writes to the log.
      */
    void mergeSelectingThread();

    /** Write the selected parts to merge into the log,
      * Call when merge_selecting_mutex is locked.
      * Returns false if any part is not in ZK.
      */
    bool createLogEntryToMergeParts(
        zkutil::ZooKeeperPtr & zookeeper,
        const MergeTreeData::DataPartsVector & parts,
        const String & merged_name,
        bool deduplicate,
        ReplicatedMergeTreeLogEntryData * out_log_entry = nullptr);

    bool createLogEntryToMutatePart(const MergeTreeDataPart & part, Int64 mutation_version);

    /// Exchange parts.

    /** Returns an empty string if no one has a part.
      */
    String findReplicaHavingPart(const String & part_name, bool active);

    /** Find replica having specified part or any part that covers it.
      * If active = true, consider only active replicas.
      * If found, returns replica name and set 'entry->actual_new_part_name' to name of found largest covering part.
      * If not found, returns empty string.
      */
    String findReplicaHavingCoveringPart(const LogEntry & entry, bool active);

    /** Download the specified part from the specified replica.
      * If `to_detached`, the part is placed in the `detached` directory.
      * If quorum != 0, then the node for tracking the quorum is updated.
      * Returns false if part is already fetching right now.
      */
    bool fetchPart(const String & part_name, const String & replica_path, bool to_detached, size_t quorum);

    /// Required only to avoid races between executeLogEntry and fetchPartition
    std::unordered_set<String> currently_fetching_parts;
    std::mutex currently_fetching_parts_mutex;

    /// With the quorum being tracked, add a replica to the quorum for the part.
    void updateQuorum(const String & part_name);

    /// Creates new block number and additionally perform precheck_ops while creates 'abandoned node'
    AbandonableLockInZooKeeper allocateBlockNumber(const String & partition_id, zkutil::ZooKeeperPtr & zookeeper,
                                                   zkutil::Requests * precheck_ops = nullptr);

    /** Wait until all replicas, including this, execute the specified action from the log.
      * If replicas are added at the same time, it can not wait the added replica .
      */
    void waitForAllReplicasToProcessLogEntry(const ReplicatedMergeTreeLogEntryData & entry);

    /** Wait until the specified replica executes the specified action from the log.
      */
    void waitForReplicaToProcessLogEntry(const String & replica_name, const ReplicatedMergeTreeLogEntryData & entry);

    /// Choose leader replica, send requst to it and wait.
    void sendRequestToLeaderReplica(const ASTPtr & query, const Settings & settings);

    /// Throw an exception if the table is readonly.
    void assertNotReadonly() const;

    /// The name of an imaginary part covering all parts in the specified partition (at the call moment).
    /// Returns empty string if the partition doesn't exist yet.
    String getFakePartNameCoveringAllPartsInPartition(
        const String & partition_id, Int64 * out_min_block = nullptr, Int64 * out_max_block = nullptr);

    /// Check for a node in ZK. If it is, remember this information, and then immediately answer true.
    std::unordered_set<std::string> existing_nodes_cache;
    std::mutex existing_nodes_cache_mutex;
    bool existsNodeCached(const std::string & path);

    /// Remove block IDs from `blocks/` in ZooKeeper for the given partition ID in the given block number range.
    void clearBlocksInPartition(
        zkutil::ZooKeeper & zookeeper, const String & partition_id, Int64 min_block_num, Int64 max_block_num);

    /// Info about how other replicas can access this one.
    ReplicatedMergeTreeAddress getReplicatedMergeTreeAddress() const;

protected:
    /** If not 'attach', either creates a new table in ZK, or adds a replica to an existing table.
      */
    StorageReplicatedMergeTree(
        const String & zookeeper_path_,
        const String & replica_name_,
        bool attach,
        const String & path_, const String & database_name_, const String & name_,
        const ColumnsDescription & columns_,
        Context & context_,
        const ASTPtr & primary_expr_ast_,
        const ASTPtr & secondary_sorting_expr_list_,
        const String & date_column_name,
        const ASTPtr & partition_expr_ast_,
        const ASTPtr & sampling_expression_,
        const MergeTreeData::MergingParams & merging_params_,
        const MergeTreeSettings & settings_,
        bool has_force_restore_data_flag);
};


extern const int MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER;

}
