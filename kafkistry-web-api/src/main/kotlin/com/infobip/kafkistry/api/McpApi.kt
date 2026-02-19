package com.infobip.kafkistry.api

import com.infobip.kafkistry.appinfo.ModulesBuildInfoLoader
import com.infobip.kafkistry.kafka.KafkaAclRule
import com.infobip.kafkistry.kafka.Partition
import com.infobip.kafkistry.kafka.TopicPartitionReAssignment
import com.infobip.kafkistry.kafkastate.TopicReplicaInfos
import com.infobip.kafkistry.model.*
import com.infobip.kafkistry.model.Presence
import com.infobip.kafkistry.model.ResourceRequirements
import com.infobip.kafkistry.model.TopicConfigMap
import com.infobip.kafkistry.model.TopicProperties
import com.infobip.kafkistry.service.NamedTypeCauseDescription
import com.infobip.kafkistry.service.NamedTypeQuantity
import com.infobip.kafkistry.service.OptionalValue
import com.infobip.kafkistry.service.acl.AclsRegistryService
import com.infobip.kafkistry.service.background.BackgroundJobIssuesRegistry
import com.infobip.kafkistry.service.cluster.ClustersRegistryService
import com.infobip.kafkistry.service.cluster.ClusterStatusService
import com.infobip.kafkistry.service.consumers.ConsumersService
import com.infobip.kafkistry.service.consumers.TopicMembers
import com.infobip.kafkistry.service.history.HistoryService
import com.infobip.kafkistry.service.quotas.QuotasRegistryService
import com.infobip.kafkistry.service.generator.AssignmentsDisbalance
import com.infobip.kafkistry.service.resources.BrokerDisk
import com.infobip.kafkistry.service.resources.ClusterResourcesAnalyzer
import com.infobip.kafkistry.service.resources.TopicClusterDiskUsage
import com.infobip.kafkistry.service.resources.TopicResourceRequiredUsages
import com.infobip.kafkistry.service.resources.TopicResourcesAnalyzer
import com.infobip.kafkistry.service.resources.UsageLevel
import com.infobip.kafkistry.service.scrapingstatus.ScrapingStatusService
import com.infobip.kafkistry.service.topic.*
import com.infobip.kafkistry.sql.SQLRepository
import org.springframework.web.bind.annotation.*

/**
 * MCP-optimized API endpoints for Claude Code MCP integration.
 *
 * URL scheme:
 *   /registry/... — configured/expected state from git-backed registry
 *   /inspect/...  — live cluster state compared against registry
 *
 * System endpoints (/build-info, /history, etc.) have no prefix.
 */
@RestController
@RequestMapping("\${app.http.root-path}/api/mcp")
class McpApi(
    private val topicsInspectionService: TopicsInspectionService,
    private val topicsRegistryService: TopicsRegistryService,
    private val consumersService: ConsumersService,
    private val clustersRegistryService: ClustersRegistryService,
    private val clusterStatusService: ClusterStatusService,
    private val aclsRegistryService: AclsRegistryService,
    private val quotasRegistryService: QuotasRegistryService,
    private val buildInfoLoader: ModulesBuildInfoLoader,
    private val backgroundIssuesRegistry: BackgroundJobIssuesRegistry,
    private val scrapingStatusService: ScrapingStatusService,
    private val historyService: HistoryService,
    private val sqlRepository: SQLRepository,
    private val clusterResourcesAnalyzer: ClusterResourcesAnalyzer,
    private val topicResourcesAnalyzer: TopicResourcesAnalyzer,
    private val suggestionService: OperationSuggestionService
) {

    /**
     * Lists all topic names registered in the Kafkistry registry (git-backed source of truth).
     *
     * This endpoint returns only the bare topic names without any associated metadata, configuration,
     * or cluster state. It is useful as a starting point to enumerate topics before fetching
     * detailed information on individual topics of interest.
     *
     * The registry represents the *desired/expected* state — topics defined here are intended to
     * exist on one or more Kafka clusters, as determined by their presence configuration.
     * A topic appearing here does not guarantee it actually exists on any cluster.
     *
     * Supports pagination via `limit` and `offset` parameters.
     *
     * @param limit Maximum number of names to return
     * @param offset Number of names to skip for pagination
     * @return List of topic names only
     */
    @GetMapping("/registry/topics/names")
    fun listRegistryTopicNames(
        @RequestParam("limit", required = false) limit: Int?,
        @RequestParam("offset", required = false, defaultValue = "0") offset: Int
    ): List<TopicName> {
        val allTopics = topicsRegistryService.listTopics()
        return applyPagination(allTopics, limit, offset).map { it.name }
    }

    /**
     * Lists all topics from the registry with ownership and presence metadata.
     *
     * Each entry includes: topic name, owner (team/service responsible for the topic),
     * presence type (which determines on which clusters the topic should exist — e.g., ALL_CLUSTERS,
     * INCLUDED_CLUSTERS, EXCLUDED_CLUSTERS, or TAGGED_CLUSTERS), and labels (arbitrary key-value
     * tags assigned to the topic for categorization or filtering).
     *
     * This endpoint is the preferred starting point when you need to browse or search topics
     * by ownership, cluster targeting strategy, or labels without loading full configuration details.
     *
     * Supports pagination via `limit` and `offset` parameters.
     *
     * @param limit Maximum number of topics to return
     * @param offset Number of topics to skip for pagination
     * @return List of topic summaries with owner, presence type, and labels
     */
    @GetMapping("/registry/topics/summary")
    fun listRegistryTopicsSummary(
        @RequestParam("limit", required = false) limit: Int?,
        @RequestParam("offset", required = false, defaultValue = "0") offset: Int
    ): List<RegistryTopicSummary> {
        val allTopics = topicsRegistryService.listTopics()
        return applyPagination(allTopics, limit, offset).map { topic ->
            RegistryTopicSummary(
                name = topic.name,
                owner = topic.owner,
                presenceType = topic.presence.type.name,
                labels = topic.labels
            )
        }
    }

    /**
     * Returns basic metadata for a single topic from the registry, excluding configuration details.
     *
     * The response includes:
     * - name: the Kafka topic name
     * - owner: the team or service responsible for the topic
     * - description: a human-readable explanation of the topic's purpose
     * - labels: arbitrary key-value tags for categorization or tooling integration
     * - producer: the service or component that writes records to the topic
     * - presence: the cluster-targeting policy (which clusters this topic should exist on)
     * - resourceRequirements: declared retention, throughput, and storage expectations
     *
     * Use this endpoint when you need to understand the business context of a topic
     * (purpose, ownership, producer) without loading configuration or cluster state.
     *
     * @param topicName Topic name
     * @return Basic topic metadata from registry
     */
    @GetMapping("/registry/topic/{topicName}/info")
    fun getRegistryTopicInfo(
        @PathVariable topicName: TopicName
    ): TopicBasicInfo {
        val desc = topicsRegistryService.getTopic(topicName)
        return TopicBasicInfo(
            name = desc.name,
            owner = desc.owner,
            description = desc.description,
            labels = desc.labels,
            producer = desc.producer,
            presence = desc.presence,
            resourceRequirements = desc.resourceRequirements
        )
    }

    /**
     * Returns the full desired configuration for a topic as stored in the registry.
     *
     * The response includes:
     * - properties: base partition count and replication factor
     * - config: base Kafka topic configuration key-value map (e.g., retention.ms, cleanup.policy)
     * - perClusterProperties: partition/replication overrides specific to individual clusters
     * - perClusterConfigOverrides: Kafka config key overrides specific to individual clusters
     * - perTagProperties: partition/replication overrides for clusters matching a given tag
     * - perTagConfigOverrides: Kafka config key overrides for clusters matching a given tag
     *
     * This is the authoritative desired configuration. The inspection endpoints (e.g.,
     * /inspect/topic/{topicName}/config) show the difference between this desired configuration
     * and the actual configuration found on each cluster.
     *
     * @param topicName Topic name
     * @return Full topic configuration including base and all per-cluster/per-tag overrides
     */
    @GetMapping("/registry/topic/{topicName}/config")
    fun getRegistryTopicConfig(
        @PathVariable topicName: TopicName
    ): RegistryTopicConfig {
        val desc = topicsRegistryService.getTopic(topicName)
        return RegistryTopicConfig(
            name = desc.name,
            properties = desc.properties,
            config = desc.config,
            perClusterProperties = desc.perClusterProperties,
            perClusterConfigOverrides = desc.perClusterConfigOverrides,
            perTagProperties = desc.perTagProperties,
            perTagConfigOverrides = desc.perTagConfigOverrides
        )
    }

    /**
     * Returns the presence (cluster-targeting) configuration for a topic from the registry.
     *
     * The presence object defines on which clusters the topic is expected to exist:
     * - ALL_CLUSTERS: the topic should be present on every registered cluster
     * - INCLUDED_CLUSTERS: only on the explicitly listed clusters
     * - EXCLUDED_CLUSTERS: on all clusters except the listed ones
     * - TAGGED_CLUSTERS: on all clusters that carry a specific tag
     *
     * This is useful for determining the intended cluster scope of a topic without
     * loading the full configuration or any live cluster state.
     *
     * @param topicName Topic name
     * @return Topic presence configuration including type and cluster/tag targeting
     */
    @GetMapping("/registry/topic/{topicName}/presence")
    fun getRegistryTopicPresence(
        @PathVariable topicName: TopicName
    ): RegistryTopicPresence {
        val desc = topicsRegistryService.getTopic(topicName)
        return RegistryTopicPresence(
            name = desc.name,
            presence = desc.presence
        )
    }

    /**
     * Returns the identifiers of all Kafka clusters registered in the Kafkistry registry.
     *
     * Each identifier is a unique string name for a Kafka cluster managed by Kafkistry.
     * These identifiers are used as path/query parameters throughout all other API endpoints
     * that operate on a per-cluster basis (e.g., inspection, disk usage, consumer groups).
     *
     * Use this endpoint to discover what clusters are managed before querying cluster-specific data.
     *
     * @return List of cluster identifiers
     */
    @GetMapping("/registry/clusters/identifiers")
    fun listRegistryClusterIdentifiers(): List<KafkaClusterIdentifier> {
        return clustersRegistryService.listClusters().map { it.identifier }
    }

    /**
     * Returns all registered clusters with their identifiers and associated tags.
     *
     * Tags are string labels assigned to clusters in the registry and are used by the topic
     * presence system (TAGGED_CLUSTERS presence type) to determine which topics should exist
     * on which clusters. For example, clusters tagged "eu" or "production" can be targeted
     * collectively by topics configured with that tag.
     *
     * Use this endpoint to understand cluster groupings and to map tag-based topic presence
     * rules to concrete clusters.
     *
     * @return List of all clusters with their identifier and tag list
     */
    @GetMapping("/registry/clusters/tags")
    fun listRegistryClusterTags(): List<ClusterSummaryInfo> {
        return clustersRegistryService.listClusters().map { cluster ->
            ClusterSummaryInfo(
                identifier = cluster.identifier,
                tags = cluster.tags
            )
        }
    }

    /**
     * Returns the configuration for a single Kafka cluster as stored in the registry.
     *
     * The response includes:
     * - identifier: the unique cluster name used throughout all Kafkistry APIs
     * - connectionString: the bootstrap server address(es) for connecting to the cluster
     * - tags: string labels used for cluster grouping and tag-based topic presence targeting
     * - sslEnabled: whether the cluster requires SSL/TLS transport encryption
     * - saslEnabled: whether the cluster requires SASL authentication
     * - profiles: named configuration profiles applied to this cluster (e.g., for custom broker settings)
     *
     * This endpoint is the source of truth for cluster connectivity and security settings
     * as configured in the registry.
     *
     * @param clusterIdentifier Cluster identifier
     * @return Full cluster configuration from registry
     */
    @GetMapping("/registry/cluster/{clusterIdentifier}/config")
    fun getRegistryClusterConfig(
        @PathVariable clusterIdentifier: KafkaClusterIdentifier
    ): ClusterConfigurationInfo {
        val cluster = clustersRegistryService.getCluster(clusterIdentifier)
        return ClusterConfigurationInfo(
            identifier = cluster.identifier,
            connectionString = cluster.connectionString,
            tags = cluster.tags,
            sslEnabled = cluster.sslEnabled,
            saslEnabled = cluster.saslEnabled,
            profiles = cluster.profiles
        )
    }

    /**
     * Returns the identifiers of all ACL principals registered in the Kafkistry registry.
     *
     * Each principal represents a Kafka security principal (e.g., "User:service-account-name")
     * for which ACL rules have been defined in the registry. These identifiers can be used with
     * /registry/acls/{principal} to fetch the full ACL rule set for a specific principal.
     *
     * The registry defines the *desired* ACL state; the actual state on each cluster may differ
     * and can be inspected via the inspection endpoints.
     *
     * @return List of all ACL principal identifiers
     */
    @GetMapping("/registry/acls/principals")
    fun listRegistryAclPrincipals(): List<PrincipalId> {
        return aclsRegistryService.listAllPrincipalsAcls().map { it.principal }
    }

    /**
     * Returns the full set of ACL rules for a specific principal as stored in the registry.
     *
     * The response is a PrincipalAclRules object which contains the principal identifier and
     * the list of ACL rule entries. Each rule specifies a resource type (TOPIC, GROUP, CLUSTER, etc.),
     * resource name pattern, operation (READ, WRITE, DESCRIBE, etc.), and permission type
     * (ALLOW or DENY).
     *
     * The presence field on each rule determines on which clusters the ACL should be applied.
     * This is the desired/expected ACL state; comparison with the actual live cluster state
     * is performed by the inspection layer.
     *
     * @param principal Principal identifier (e.g., "User:my-service")
     * @return All ACL rules declared for this principal in the registry
     */
    @GetMapping("/registry/acls/{principal}")
    fun getRegistryPrincipalAcls(
        @PathVariable principal: PrincipalId
    ): PrincipalAclRules {
        return aclsRegistryService.getPrincipalAcls(principal)
    }

    /**
     * Returns the identifiers of all client quota entities registered in the Kafkistry registry.
     *
     * A quota entity identifies the target of a Kafka client quota and can represent:
     * - a specific user (e.g., "User:alice")
     * - a specific client ID (e.g., "ClientId:my-producer")
     * - a combination of user and client ID
     * - default quotas for all users or all clients
     *
     * These entity IDs can be used with /registry/quotas/{quotaEntityID} to fetch the
     * full quota configuration for a specific entity.
     *
     * @return List of all quota entity IDs registered in the registry
     */
    @GetMapping("/registry/quotas/entities")
    fun listRegistryQuotaEntities(): List<QuotaEntityID> {
        return quotasRegistryService.listAllQuotas().map { it.entity.asID() }
    }

    /**
     * Returns all client quota definitions from the registry.
     *
     * Each QuotaDescription contains:
     * - entity: identifies the target (user, client ID, or combination) the quota applies to
     * - presence: the cluster-targeting policy (which clusters the quota should be applied on)
     * - properties: the actual quota values such as producer_byte_rate, consumer_byte_rate,
     *   and request_percentage limits
     *
     * This is the desired quota state as configured in the registry. Actual quota state on
     * each cluster is managed and reconciled by the inspection and autopilot layers.
     *
     * @return All quota descriptions from the registry
     */
    @GetMapping("/registry/quotas")
    fun listRegistryQuotas(): List<QuotaDescription> {
        return quotasRegistryService.listAllQuotas()
    }

    /**
     * Returns the quota definition for a specific entity from the registry.
     *
     * The response includes the entity identifier, the cluster presence configuration
     * (which clusters the quota applies to), and the quota property values (e.g.,
     * producer_byte_rate, consumer_byte_rate, request_percentage).
     *
     * Use /registry/quotas/entities to discover valid quota entity IDs before calling this endpoint.
     *
     * @param quotaEntityID Quota entity identifier (e.g., "User:alice" or "ClientId:producer-app")
     * @return Quota description for the specified entity
     */
    @GetMapping("/registry/quotas/{quotaEntityID}")
    fun getRegistryQuota(
        @PathVariable quotaEntityID: QuotaEntityID
    ): QuotaDescription {
        return quotasRegistryService.getQuotas(quotaEntityID)
    }

    /**
     * Returns the live inspection result for a single topic, combining registry expectations
     * with the actual observed state on each cluster.
     *
     * The response contains:
     * - topicName: the topic being inspected
     * - topicDescription: the registry definition for this topic (may be null if topic is unknown)
     * - aggStatusFlags: aggregated boolean flags across all clusters (allOk, hasWarnings, hasErrors, etc.)
     * - statusPerClusters: per-cluster inspection status, each including the list of status type codes
     *   such as OK, MISSING, UNEXPECTED, WRONG_CONFIG, WRONG_PARTITION_COUNT, CONFIG_RULE_VIOLATION, etc.
     *
     * Status types signal the relationship between the desired (registry) and actual (cluster) state.
     * A topic can be OK on some clusters and MISSING or misconfigured on others.
     *
     * The optional `clustersOnly` parameter accepts a comma-separated list of cluster identifiers
     * to restrict the response to specific clusters, which is useful when you only care about
     * the status on a subset of clusters.
     *
     * @param topicName Topic name
     * @param clustersOnly Comma-separated cluster identifiers to filter by (optional)
     * @return Minimal topic inspection status, or null if topic not found
     */
    @GetMapping("/inspect/topic/{topicName}")
    fun inspectTopic(
        @PathVariable topicName: TopicName,
        @RequestParam("clustersOnly", required = false) clustersOnly: String?
    ): MinimalTopicStatuses? {
        val topic = topicsInspectionService.inspectAllTopics()
            .firstOrNull { it.topicName == topicName }
            ?: return null

        val clusterIds = clustersOnly?.split(",")?.toSet()
        val clusters = if (clusterIds != null) {
            topic.statusPerClusters.filter { it.clusterIdentifier in clusterIds }
        } else {
            topic.statusPerClusters
        }

        return MinimalTopicStatuses(
            topicName = topic.topicName,
            topicDescription = topic.topicDescription,
            aggStatusFlags = topic.aggStatusFlags,
            statusPerClusters = clusters.map { cluster ->
                MinimalTopicClusterStatus(
                    status = MinimalTopicOnClusterInspectionResult(types = cluster.status.types),
                    clusterIdentifier = cluster.clusterIdentifier
                )
            }
        )
    }

    /**
     * Returns a compact health summary across all topics and clusters.
     *
     * For each topic, the response provides:
     * - topicName: the topic name
     * - overallOk: whether the topic has no issues on any cluster
     * - clusterStatuses: per-cluster breakdown with an ok flag and a count of active issue types
     *
     * This endpoint is the most efficient way to scan the full fleet of topics for problems.
     * It does not return issue details — use /inspect/topic/{topicName}/status for the list of
     * specific status types, or /inspect/topic/{topicName}/config for configuration diff details.
     *
     * The optional `clustersOnly` parameter accepts comma-separated cluster identifiers
     * to limit the per-cluster breakdown to specific clusters.
     *
     * Supports pagination via `limit` and `offset` parameters.
     *
     * @param limit Maximum number of topics to return
     * @param offset Number of topics to skip for pagination
     * @param clustersOnly Comma-separated cluster identifiers to filter by (optional)
     * @return List of per-topic health summaries
     */
    @GetMapping("/inspect/topics/status-summary")
    fun inspectTopicsStatusSummary(
        @RequestParam("limit", required = false) limit: Int?,
        @RequestParam("offset", required = false, defaultValue = "0") offset: Int,
        @RequestParam("clustersOnly", required = false) clustersOnly: String?
    ): List<TopicStatusSummary> {
        val allTopics = topicsInspectionService.inspectAllTopics()
        val clusterIds = clustersOnly?.split(",")?.toSet()

        val summaries = allTopics.map { topic ->
            val filteredClusters = if (clusterIds != null) {
                topic.statusPerClusters.filter { it.clusterIdentifier in clusterIds }
            } else {
                topic.statusPerClusters
            }
            TopicStatusSummary(
                topicName = topic.topicName,
                overallOk = topic.aggStatusFlags.allOk,
                clusterStatuses = filteredClusters.map { cluster ->
                    ClusterStatusSummary(
                        clusterIdentifier = cluster.clusterIdentifier,
                        ok = cluster.status.flags.allOk,
                        issueCount = cluster.status.types.size
                    )
                }
            )
        }

        return applyPagination(summaries, limit, offset)
    }

    /**
     * Returns detailed inspection status flags and issue type codes for a topic across all clusters.
     *
     * The response includes:
     * - topicName: the topic being inspected
     * - aggStatusFlags: aggregated boolean health flags across all clusters
     *   (allOk, hasWarnings, hasErrors, hasConfigIssues, hasPartitionIssues, etc.)
     * - availableActions: the set of remediation actions Kafkistry can perform on this topic
     *   (e.g., CREATE, DELETE, ALTER_PARTITION_COUNT, ALTER_CONFIG)
     * - clusterStatuses: per-cluster status detail including:
     *   - flags: per-cluster boolean health flags
     *   - types: list of status type string codes (OK, MISSING, UNEXPECTED, WRONG_CONFIG,
     *     WRONG_PARTITION_COUNT, WRONG_REPLICATION_FACTOR, CONFIG_RULE_VIOLATION, etc.)
     *   - exists: whether the topic physically exists on this cluster
     *   - lastRefreshTime: epoch millis of the most recent cluster state scrape
     *
     * This endpoint is the best source for understanding what is wrong with a topic's state
     * and what actions are available for remediation. For the specific configuration values
     * that differ, use /inspect/topic/{topicName}/config.
     *
     * @param topicName Topic name
     * @return Status flags, issue type codes, and available actions per cluster
     */
    @GetMapping("/inspect/topic/{topicName}/status")
    fun inspectTopicStatus(
        @PathVariable topicName: TopicName
    ): TopicStatusInfo {
        val topicStatus = topicsInspectionService.inspectTopic(topicName)
        return TopicStatusInfo(
            topicName = topicStatus.topicName,
            aggStatusFlags = topicStatus.aggStatusFlags,
            availableActions = topicStatus.availableActions,
            clusterStatuses = topicStatus.statusPerClusters.map {
                TopicClusterStatusInfo(
                    clusterIdentifier = it.clusterIdentifier,
                    flags = it.status.flags,
                    types = it.status.types.map { type -> type.name },
                    exists = it.status.exists,
                    lastRefreshTime = it.lastRefreshTime
                )
            }
        )
    }

    /**
     * Returns the configuration diff between desired (registry) and actual (cluster) state for a topic.
     *
     * For each cluster the topic is inspected on, the response lists:
     * - wrongValues: configuration entries where the actual value on the cluster differs from
     *   the desired value in the registry. Each entry includes the key, expected value, and actual value.
     * - updateValues: configuration entries that are scheduled to be changed (e.g., a registry
     *   change not yet applied to the cluster).
     * - ruleViolations: configuration entries that violate a Kafkistry validation rule
     *   (e.g., retention.ms exceeds the allowed maximum for a given topic type).
     * - currentConfigRuleViolations: rule violations based on what is currently on the cluster
     *   (as opposed to what the registry requests).
     *
     * This endpoint is the primary source for diagnosing WRONG_CONFIG status issues identified
     * by /inspect/topic/{topicName}/status. It shows the exact configuration keys and values
     * that are out of alignment.
     *
     * @param topicName Topic name
     * @return Per-cluster configuration diffs and rule violations
     */
    @GetMapping("/inspect/topic/{topicName}/config")
    fun inspectTopicConfig(
        @PathVariable topicName: TopicName
    ): TopicInspectConfigInfo {
        val topicStatus = topicsInspectionService.inspectTopic(topicName)
        return TopicInspectConfigInfo(
            topicName = topicStatus.topicName,
            clusterConfigIssues = topicStatus.statusPerClusters.map {
                ClusterConfigIssues(
                    clusterIdentifier = it.clusterIdentifier,
                    wrongValues = it.status.wrongValues ?: emptyList(),
                    updateValues = it.status.updateValues ?: emptyList(),
                    ruleViolations = it.status.ruleViolations ?: emptyList(),
                    currentConfigRuleViolations = it.status.currentConfigRuleViolations ?: emptyList()
                )
            }
        )
    }

    /**
     * Returns the deduplicated set of ACL rules that affect this topic across all clusters.
     *
     * The response contains all KafkaAclRule entries from the live inspection state that
     * match this topic by resource name or pattern. Each rule includes:
     * - principal: the security principal the rule applies to (e.g., "User:my-service")
     * - resource: the resource type and name/pattern (e.g., TOPIC with name "my-topic")
     * - operation: the operation being permitted or denied (e.g., READ, WRITE, DESCRIBE)
     * - permission: ALLOW or DENY
     * - host: the host from which this rule applies ("*" means any host)
     *
     * Rules are aggregated across all clusters and deduplicated by (principal, resource, operation),
     * providing a unified view of who has what access to the topic.
     *
     * This is derived from the live cluster inspection state, not the registry definition.
     * Use /registry/acls/{principal} to see registry-desired ACL rules for a specific principal.
     *
     * @param topicName Topic name
     * @return Deduplicated ACL rules affecting this topic from live cluster state
     */
    @GetMapping("/inspect/topic/{topicName}/acls")
    fun inspectTopicAcls(
        @PathVariable topicName: TopicName
    ): TopicAclInfo {
        val topicStatus = topicsInspectionService.inspectTopic(topicName)
        val allAclRules = topicStatus.statusPerClusters
            .flatMap { it.status.affectingAclRules }
            .distinctBy { it.principal to it.resource to it.operation }
        return TopicAclInfo(
            topicName = topicStatus.topicName,
            affectingAclRules = allAclRules
        )
    }

    /**
     * Returns live partition assignment details and rebalancing state for a topic across all clusters.
     *
     * For each cluster where the topic exists, the response provides:
     * - existingTopicInfo: the live state of the topic as observed on the cluster, including:
     *   - actual partition count and replication factor
     *   - per-partition assignment map (partition index → list of broker IDs holding replicas)
     *   - leader, in-sync replicas (ISR), and offline replicas per partition
     *   - current Kafka topic configuration values
     * - assignmentsDisbalance: analysis of how evenly partitions and leaders are distributed
     *   across brokers. Includes metrics like partitionCount imbalance, leadersCount imbalance,
     *   and replication imbalance per broker. A value of null means no disbalance was detected.
     * - currentReAssignments: any in-progress partition reassignment operations on this topic,
     *   keyed by partition index. Shows the add/remove replica movements currently underway.
     *
     * This endpoint is useful for diagnosing partition distribution problems, verifying replication
     * health, and monitoring ongoing reassignment operations.
     *
     * @param topicName Topic name
     * @return Partition assignments, replica states, and rebalancing info per cluster
     */
    @GetMapping("/inspect/topic/{topicName}/assignments")
    fun inspectTopicAssignments(
        @PathVariable topicName: TopicName
    ): TopicAssignmentInfo {
        val topicStatus = topicsInspectionService.inspectTopic(topicName)
        return TopicAssignmentInfo(
            topicName = topicStatus.topicName,
            clusterAssignments = topicStatus.statusPerClusters
                .filter { it.existingTopicInfo != null }
                .associate { cluster ->
                    cluster.clusterIdentifier to ClusterAssignmentDetails(
                        existingTopicInfo = cluster.existingTopicInfo!!,
                        assignmentsDisbalance = cluster.status.assignmentsDisbalance,
                        currentReAssignments = cluster.currentReAssignments
                    )
                }
        )
    }

    /**
     * Returns the resource requirements and actual measured resource usage for a topic across clusters.
     *
     * The response includes:
     * - resourceRequirements: the declared requirements from the registry, expressing the expected
     *   throughput (messagesPerSecond, bytesPerSecond), message size, retention, and replication
     *   factor. These are the topic owner's stated expectations used for capacity planning.
     * - clusterResources: for each cluster where resource data is available, the actual computed
     *   usage including:
     *   - requiredRetentionBytes: total storage required given replication and retention settings
     *   - requiredProduceBytesPerSec: write throughput demand
     *   - requiredDiskBytesPerSec: total disk I/O requirement accounting for replication
     *   - usageLevel: a severity classification (LOW, MEDIUM, HIGH, VERY_HIGH) based on
     *     the topic's resource footprint relative to available cluster capacity
     *
     * Use this endpoint to understand storage and throughput requirements before making
     * configuration changes, or to identify topics with unexpectedly high resource footprints.
     *
     * @param topicName Topic name
     * @return Registry resource requirements and actual per-cluster resource usage metrics
     */
    @GetMapping("/inspect/topic/{topicName}/resources")
    fun inspectTopicResources(
        @PathVariable topicName: TopicName
    ): TopicResourceInfo {
        val topicStatus = topicsInspectionService.inspectTopic(topicName)
        return TopicResourceInfo(
            topicName = topicStatus.topicName,
            resourceRequirements = topicStatus.topicDescription?.resourceRequirements,
            clusterResources = topicStatus.statusPerClusters
                .filter { it.resourceRequiredUsages.value != null }
                .associate { it.clusterIdentifier to it.resourceRequiredUsages.value!! }
        )
    }

    /**
     * Returns the current connectivity and scraping state for all registered clusters.
     *
     * For each cluster, the response provides:
     * - identifier: the cluster's unique name
     * - stateType: the current state of the cluster as seen by Kafkistry's scraping system.
     *   Common values are: VISIBLE (cluster is reachable and data is fresh), UNREACHABLE
     *   (connection failures), DISABLED (scraping is turned off for this cluster)
     * - ok: a convenience boolean that is true when stateType is VISIBLE
     * - lastRefreshTime: epoch millis of the most recent successful or attempted state scrape
     *
     * Use this endpoint to quickly check which clusters are healthy and reachable before
     * querying cluster-specific inspection data. Clusters that are not VISIBLE may return
     * stale or absent data from other inspection endpoints.
     *
     * @return Current connectivity state for all registered clusters
     */
    @GetMapping("/inspect/clusters/all-statuses")
    fun inspectClustersAllStatuses(): List<ClusterStatusOverview> {
        return clusterStatusService.clustersState().map { clusterState ->
            ClusterStatusOverview(
                identifier = clusterState.cluster.identifier,
                stateType = clusterState.clusterState.name,
                ok = clusterState.clusterState.name == "VISIBLE",
                lastRefreshTime = clusterState.lastRefreshTime
            )
        }
    }

    /**
     * Returns the live state and broker topology for a specific Kafka cluster.
     *
     * The response includes:
     * - identifier: the cluster's unique name
     * - stateType: current scraping state (VISIBLE, UNREACHABLE, DISABLED, etc.)
     * - clusterInfo: live broker details when the cluster is reachable:
     *   - nodeIds: list of all broker IDs in the cluster
     *   - controllerId: the broker ID currently acting as the cluster controller
     *   - version: the Kafka broker version string (e.g., "3.9.0")
     *   - securityEnabled: whether SSL or SASL security is active on the cluster
     * - lastRefreshTime: epoch millis of the most recent state scrape
     *
     * This endpoint provides the ground-truth view of the cluster's broker membership and
     * version as observed at scrape time. The clusterInfo field will be null if the cluster
     * is not currently reachable (stateType is not VISIBLE).
     *
     * @param clusterIdentifier Cluster identifier
     * @return Live broker topology and cluster metadata, or null if cluster is not registered
     */
    @GetMapping("/inspect/cluster/{clusterIdentifier}/state")
    fun inspectClusterState(
        @PathVariable clusterIdentifier: KafkaClusterIdentifier
    ): ClusterStateInfo? {
        val clusterState = clusterStatusService.clustersState()
            .firstOrNull { it.cluster.identifier == clusterIdentifier }
            ?: return null
        return ClusterStateInfo(
            identifier = clusterIdentifier,
            stateType = clusterState.clusterState.name,
            clusterInfo = clusterState.clusterInfo?.let { info ->
                ClusterInfoMinimal(
                    nodeIds = info.nodeIds,
                    controllerId = info.controllerId,
                    version = info.clusterVersion?.toString(),
                    securityEnabled = info.securityEnabled
                )
            },
            lastRefreshTime = clusterState.lastRefreshTime
        )
    }

    /**
     * Returns broker count statistics for a specific Kafka cluster.
     *
     * The response includes:
     * - identifier: the cluster's unique name
     * - brokerCount: the total number of broker nodes registered in the cluster metadata
     * - onlineBrokerCount: the number of brokers currently reachable and participating in the cluster
     *
     * A difference between brokerCount and onlineBrokerCount indicates that one or more brokers
     * are offline or not responding. This is a quick health check for broker availability without
     * fetching the full cluster state.
     *
     * Returns null if the cluster is not registered or if cluster info is unavailable
     * (e.g., when the cluster is unreachable).
     *
     * @param clusterIdentifier Cluster identifier
     * @return Broker count statistics, or null if cluster state is unavailable
     */
    @GetMapping("/inspect/cluster/{clusterIdentifier}/stats")
    fun inspectClusterStats(
        @PathVariable clusterIdentifier: KafkaClusterIdentifier
    ): ClusterStatisticsInfo? {
        val clusterState = clusterStatusService.clustersState()
            .firstOrNull { it.cluster.identifier == clusterIdentifier }
            ?: return null
        val clusterInfo = clusterState.clusterInfo ?: return null
        return ClusterStatisticsInfo(
            identifier = clusterIdentifier,
            brokerCount = clusterInfo.nodeIds.size,
            onlineBrokerCount = clusterInfo.onlineNodeIds.size
        )
    }

    /**
     * Returns aggregated disk usage statistics for all topics on a cluster (no per-topic breakdown).
     *
     * The response includes:
     * - identifier: the cluster being analyzed
     * - combined: aggregated disk statistics across all brokers in the cluster, including:
     *   - totalCapacityBytes: total disk capacity available
     *   - usedBytes: bytes currently occupied by Kafka log segments
     *   - freeBytes: bytes remaining before the disk is full
     *   - possibleUsedBytes: projected bytes if all topics reach their configured retention limits
     * - worstCurrentUsageLevel: the most severe UsageLevel (LOW/MEDIUM/HIGH/VERY_HIGH/CRITICAL)
     *   seen among brokers based on their current disk fill ratio
     * - worstPossibleUsageLevel: the most severe UsageLevel among brokers if all topics were
     *   to fill up to their configured retention limits
     * - errors: list of error messages if disk usage data could not be retrieved for some brokers
     *
     * Use /inspect/cluster/{id}/disk-usage/{topicName} to drill down into a specific topic's
     * contribution to disk usage on this cluster.
     *
     * @param clusterIdentifier Cluster identifier
     * @return Aggregated cluster-level disk usage statistics
     */
    @GetMapping("/inspect/cluster/{clusterIdentifier}/disk-usage")
    fun inspectClusterDiskUsage(
        @PathVariable clusterIdentifier: KafkaClusterIdentifier
    ): ClusterTotalDiskUsage {
        val diskUsage = clusterResourcesAnalyzer.clusterDiskUsage(clusterIdentifier)
        return ClusterTotalDiskUsage(
            identifier = clusterIdentifier,
            combined = diskUsage.combined,
            worstCurrentUsageLevel = diskUsage.worstCurrentUsageLevel,
            worstPossibleUsageLevel = diskUsage.worstPossibleUsageLevel,
            errors = diskUsage.errors
        )
    }

    /**
     * Returns disk usage details for a specific topic on a specific cluster.
     *
     * The response includes:
     * - identifier: the cluster being analyzed
     * - topicName: the topic being analyzed
     * - diskUsage: an OptionalValue wrapping the TopicClusterDiskUsage, which contains:
     *   - combined: aggregated disk usage for this topic across all brokers (total bytes on disk)
     *   - perBroker: per-broker breakdown showing how many bytes of this topic's data each broker holds
     *   - retentionBoundedSizeBytes: estimated maximum size this topic can reach given its
     *     retention configuration
     *   - configuredReplicationFactor: the replication factor in effect, which multiplies storage
     *
     * Returns null if the cluster is not registered. The diskUsage value itself may be absent
     * (OptionalValue with no value) if disk usage data for this topic is not available on the cluster.
     *
     * @param clusterIdentifier Cluster identifier
     * @param topicName Topic name
     * @return Topic disk usage on the cluster, or null if the cluster is not registered
     */
    @GetMapping("/inspect/cluster/{clusterIdentifier}/disk-usage/{topicName}")
    fun inspectClusterTopicDiskUsage(
        @PathVariable clusterIdentifier: KafkaClusterIdentifier,
        @PathVariable topicName: TopicName
    ): ClusterTopicDiskUsageResult? {
        val diskUsage = clusterResourcesAnalyzer.clusterDiskUsage(clusterIdentifier)
        val topicUsage = diskUsage.topicDiskUsages[topicName] ?: return null
        return ClusterTopicDiskUsageResult(
            identifier = clusterIdentifier,
            topicName = topicName,
            diskUsage = topicUsage
        )
    }

    /**
     * Returns the consumer group IDs observed on each cluster.
     *
     * The response is a map from cluster identifier to a list of consumer group IDs active
     * on that cluster. This represents the live observed state from the most recent cluster
     * state scrape — it is not derived from the registry.
     *
     * The optional `clusterIdentifier` parameter restricts the result to a single cluster.
     * When omitted, all clusters are included.
     *
     * Use this endpoint to discover what consumer groups exist before fetching detailed
     * information with /inspect/consumer/{clusterIdentifier}/{consumerGroupId}/summary or
     * related endpoints.
     *
     * @param clusterIdentifier Optional cluster identifier to filter results to a single cluster
     * @return Map of cluster identifier to list of consumer group IDs on that cluster
     */
    @GetMapping("/inspect/consumers/names")
    fun inspectConsumerGroupNames(
        @RequestParam("clusterIdentifier", required = false) clusterIdentifier: KafkaClusterIdentifier?
    ): Map<KafkaClusterIdentifier, List<ConsumerGroupId>> {
        val consumersData = consumersService.allConsumersData()
        val filtered = if (clusterIdentifier != null) {
            consumersData.clustersGroups.filter { it.clusterIdentifier == clusterIdentifier }
        } else {
            consumersData.clustersGroups
        }
        return filtered
            .groupBy { it.clusterIdentifier }
            .mapValues { (_, groups) -> groups.map { it.consumerGroup.groupId } }
    }

    /**
     * Returns a high-level summary of a consumer group on a specific cluster.
     *
     * The response includes:
     * - groupId: the consumer group identifier
     * - clusterIdentifier: the cluster this group was observed on
     * - status: the current group state as reported by Kafka (e.g., Stable, Empty, PreparingRebalance,
     *   CompletingRebalance, Dead)
     * - lagAmount: the total number of unconsumed messages across all partitions and topics
     *   this group consumes. May be null if offsets or end offsets could not be fetched.
     * - lagStatus: a severity classification of the lag (e.g., OK, WARNING, CRITICAL)
     * - partitionAssignor: the partition assignment strategy in use (e.g., range, roundrobin,
     *   sticky, cooperative-sticky)
     * - topics: list of topic names this group is currently consuming
     *
     * Use /inspect/consumer/{clusterIdentifier}/{consumerGroupId}/lag for per-topic lag breakdown,
     * or /inspect/consumer/{clusterIdentifier}/{consumerGroupId}/topics for full partition-level detail.
     *
     * @param clusterIdentifier Cluster identifier
     * @param consumerGroupId Consumer group ID
     * @return Consumer group summary, or null if the group is not found on this cluster
     */
    @GetMapping("/inspect/consumer/{clusterIdentifier}/{consumerGroupId}/summary")
    fun inspectConsumerGroupSummary(
        @PathVariable clusterIdentifier: KafkaClusterIdentifier,
        @PathVariable consumerGroupId: ConsumerGroupId
    ): ConsumerGroupSummary? {
        val group = consumersService.consumerGroup(clusterIdentifier, consumerGroupId)
            ?: return null
        return ConsumerGroupSummary(
            groupId = group.groupId,
            clusterIdentifier = clusterIdentifier,
            status = group.status.name,
            lagAmount = group.lag.amount,
            lagStatus = group.lag.status.name,
            partitionAssignor = group.partitionAssignor,
            topics = group.topicMembers.map { it.topicName }
        )
    }

    /**
     * Returns consumer group lag information broken down by topic, without partition-level detail.
     *
     * The response includes:
     * - groupId: the consumer group identifier
     * - clusterIdentifier: the cluster this group was observed on
     * - totalLag: the sum of unconsumed messages across all partitions and all topics this group
     *   consumes. May be null if offsets could not be determined.
     * - lagStatus: a severity classification of the total lag (e.g., OK, WARNING, CRITICAL)
     * - topicLags: a map from topic name to the total lag for that topic (sum across all
     *   partitions of the topic). Values may be null if end offsets could not be fetched.
     *
     * This endpoint is useful for identifying which specific topics within a consumer group
     * are accumulating backlog. For partition-level detail (individual offsets per partition),
     * use /inspect/consumer/{clusterIdentifier}/{consumerGroupId}/topics.
     *
     * @param clusterIdentifier Cluster identifier
     * @param consumerGroupId Consumer group ID
     * @return Total and per-topic lag amounts, or null if the group is not found on this cluster
     */
    @GetMapping("/inspect/consumer/{clusterIdentifier}/{consumerGroupId}/lag")
    fun inspectConsumerGroupLag(
        @PathVariable clusterIdentifier: KafkaClusterIdentifier,
        @PathVariable consumerGroupId: ConsumerGroupId
    ): ConsumerGroupLagInfo? {
        val group = consumersService.consumerGroup(clusterIdentifier, consumerGroupId)
            ?: return null
        return ConsumerGroupLagInfo(
            groupId = group.groupId,
            clusterIdentifier = clusterIdentifier,
            totalLag = group.lag.amount,
            lagStatus = group.lag.status.name,
            topicLags = group.topicMembers.associate { it.topicName to it.lag.amount }
        )
    }

    /**
     * Returns the full partition-level membership details for a consumer group on a cluster.
     *
     * The response contains a list of TopicMembers, one per topic consumed by this group.
     * Each TopicMembers entry includes:
     * - topicName: the topic being consumed
     * - lag: aggregate lag for the topic across all partitions
     * - partitionMembers: per-partition detail including:
     *   - partition: the partition index
     *   - memberId: the consumer instance ID assigned to this partition (if currently assigned)
     *   - clientId / host: the client identifier and host of the consumer holding this partition
     *   - offset: the committed consumer offset for this partition
     *   - endOffset: the latest available offset (log end offset) for this partition
     *   - lag: the difference between endOffset and committed offset for this partition
     *
     * This is the most detailed consumer group endpoint and provides full visibility into
     * per-partition assignment, offset commits, and individual consumer membership.
     * It can return a large response for groups consuming many topics with many partitions.
     *
     * @param clusterIdentifier Cluster identifier
     * @param consumerGroupId Consumer group ID
     * @return Full partition-level topic member details, or null if the group is not found
     */
    @GetMapping("/inspect/consumer/{clusterIdentifier}/{consumerGroupId}/topics")
    fun inspectConsumerGroupTopicMembers(
        @PathVariable clusterIdentifier: KafkaClusterIdentifier,
        @PathVariable consumerGroupId: ConsumerGroupId
    ): ConsumerGroupTopicMembersInfo? {
        val group = consumersService.consumerGroup(clusterIdentifier, consumerGroupId)
            ?: return null
        return ConsumerGroupTopicMembersInfo(
            groupId = group.groupId,
            clusterIdentifier = clusterIdentifier,
            topicMembers = group.topicMembers
        )
    }

    /**
     * Returns the ACL rules that affect a specific consumer group on a cluster.
     *
     * The response contains all KafkaAclRule entries from the live cluster inspection state
     * that are applicable to this consumer group. Relevant ACL rules typically include those
     * with resource type GROUP and a name/pattern matching this consumer group's ID. Each rule
     * includes:
     * - principal: the security principal the rule applies to
     * - resource: resource type GROUP with the consumer group name or pattern
     * - operation: the operation (e.g., READ, DESCRIBE, DELETE)
     * - permission: ALLOW or DENY
     * - host: the source host restriction ("*" means any host)
     *
     * This is derived from the live cluster inspection state. Use this endpoint to verify
     * that a consumer group has the necessary ACL permissions to read from topics, or to
     * audit access control for a specific group.
     *
     * @param clusterIdentifier Cluster identifier
     * @param consumerGroupId Consumer group ID
     * @return ACL rules affecting this consumer group, or null if the group is not found
     */
    @GetMapping("/inspect/consumer/{clusterIdentifier}/{consumerGroupId}/acls")
    fun inspectConsumerGroupAcls(
        @PathVariable clusterIdentifier: KafkaClusterIdentifier,
        @PathVariable consumerGroupId: ConsumerGroupId
    ): ConsumerGroupAclInfo? {
        val group = consumersService.consumerGroup(clusterIdentifier, consumerGroupId)
            ?: return null
        return ConsumerGroupAclInfo(
            groupId = group.groupId,
            clusterIdentifier = clusterIdentifier,
            affectingAclRules = group.affectingAclRules
        )
    }

    /**
     * Returns build and version information for all Kafkistry modules.
     *
     * The response contains a list of module build info entries, each including the module name,
     * artifact version, build timestamp, and Git commit hash. This can be used to verify which
     * version of Kafkistry is running and whether all modules are consistently versioned.
     *
     * Useful for diagnostics, support requests, and verifying deployment versions.
     */
    @GetMapping("/build-info")
    fun getBuildInfo() = buildInfoLoader.modulesInfos()

    /**
     * Returns any currently active issues reported by Kafkistry's background processing jobs.
     *
     * Kafkistry runs background jobs for tasks such as cluster state scraping, autopilot
     * remediation, record analysis, and repository synchronization. When a background job
     * encounters an error or enters a degraded state, it registers an issue here.
     *
     * Each issue entry typically includes the job name, the error message or description,
     * and a timestamp. An empty list indicates all background jobs are operating normally.
     *
     * Use this endpoint to diagnose problems with data freshness, failed remediations,
     * or connectivity issues that are not reflected in the per-cluster scraping status.
     */
    @GetMapping("/background-issues")
    fun getBackgroundIssues() = backgroundIssuesRegistry.currentIssues()

    /**
     * Returns the current scraping status for each registered Kafka cluster.
     *
     * Kafkistry periodically polls each cluster to refresh its in-memory state (topics, ACLs,
     * quotas, consumer groups, partition assignments, disk usage, etc.). This endpoint exposes
     * the health of that scraping pipeline.
     *
     * Each entry includes the cluster identifier, when it was last successfully scraped,
     * whether the last scrape attempt succeeded or failed, and any error details if applicable.
     *
     * Use this endpoint to understand data freshness — if a cluster was last scraped a long
     * time ago or is in a failed state, inspection results for that cluster may be stale.
     */
    @GetMapping("/scraping-status")
    fun getScrapingStatus() = scrapingStatusService.currentScrapingStatuses()

    /**
     * Returns the most recent change history entries from the Kafkistry registry.
     *
     * The history represents changes committed to the git-backed registry — topic creations,
     * deletions, configuration updates, ACL changes, quota changes, cluster registrations,
     * and similar registry-level mutations. Each entry typically includes:
     * - the type of change (topic updated, ACL added, cluster registered, etc.)
     * - the name of the affected entity
     * - the author who made the change
     * - the commit message or description
     * - the timestamp of the change
     *
     * The `count` parameter controls how many recent entries are returned (default: 10).
     * Use a larger count to audit a longer window of recent activity.
     *
     * @param count Number of most recent history entries to return (default: 10)
     */
    @GetMapping("/history")
    fun getHistory(
        @RequestParam("count", required = false, defaultValue = "10") count: Int
    ) = historyService.getRecentHistory(count)

    /**
     * Executes a SQL query against Kafkistry's in-memory SQLite metadata database.
     *
     * Kafkistry maintains a queryable SQLite store populated from scraped cluster state.
     * This database contains tables for topics, partitions, consumer groups, ACLs, quotas,
     * cluster nodes, and related metadata. SQL SELECT queries can be used to perform
     * cross-cluster aggregations, filtering, and analysis that would otherwise require
     * multiple API calls.
     *
     * Example queries:
     * - SELECT name, owner FROM topics WHERE owner = 'my-team'
     * - SELECT cluster, topic, lag FROM consumer_group_offsets WHERE lag > 10000
     * - SELECT * FROM topics WHERE partition_count > 100
     *
     * The available tables and their schemas can be discovered by querying the SQLite
     * system tables (e.g., SELECT name FROM sqlite_master WHERE type='table').
     *
     * Only read (SELECT) queries are supported; write operations are not permitted.
     *
     * @param sql SQL SELECT query string
     * @return Query result as a list of rows, each row being a map of column name to value
     */
    @PostMapping("/sql")
    fun executeSqlQuery(
        @RequestParam("sql") sql: String
    ) = sqlRepository.query(sql)

    /**
     * Returns detailed disk usage analysis for a specific topic on a specific cluster.
     *
     * Unlike the cluster-centric disk usage endpoints, this endpoint is focused on the topic
     * as the primary entity and provides a detailed breakdown from the topic resource analyzer.
     * The response includes:
     * - actual bytes on disk per broker holding this topic's partitions
     * - total bytes across all brokers (accounting for replication)
     * - retention-bounded size estimate (projected maximum storage footprint)
     * - comparison with declared resource requirements if present in the registry
     * - usage level classification based on how much of the cluster's available disk this
     *   topic is consuming
     *
     * This endpoint is the most detailed source for understanding a single topic's storage
     * footprint on a given cluster and is useful for capacity planning and anomaly detection.
     *
     * @param topicName Topic name
     * @param clusterIdentifier Cluster identifier
     * @return Detailed disk usage analysis for this topic on this cluster
     */
    @GetMapping("/inspect/topic/{topicName}/disk-usage/{clusterIdentifier}")
    fun inspectTopicDiskUsage(
        @PathVariable topicName: TopicName,
        @PathVariable clusterIdentifier: KafkaClusterIdentifier
    ) = topicResourcesAnalyzer.topicOnClusterDiskUsage(topicName, clusterIdentifier)

    /**
     * Generates a suggested registry configuration for importing a topic that exists on a cluster
     * but is not yet registered in the Kafkistry registry.
     *
     * When a topic is present on a Kafka cluster but absent from the registry (status type:
     * UNEXPECTED), this endpoint analyzes the topic's actual live configuration and produces
     * a suggested TopicDescription that can be used as the basis for registering it.
     *
     * The suggestion includes:
     * - Inferred properties (partition count, replication factor) from the live cluster state
     * - Inferred configuration map from the actual topic config on the cluster
     * - A presence configuration reflecting which clusters the topic currently exists on
     * - Placeholder or inferred values for owner, description, labels, and resourceRequirements
     *
     * The returned suggestion is a starting point that should be reviewed and adjusted before
     * being submitted to create the official registry entry. It does not automatically register
     * the topic.
     *
     * @param topicName Topic name to generate an import suggestion for
     * @return Suggested TopicDescription for registering this topic in the registry
     */
    @GetMapping("/registry/topic/{topicName}/import-suggestion")
    fun suggestTopicImport(
        @PathVariable topicName: TopicName
    ) = suggestionService.suggestTopicImport(topicName)

    private fun <T> applyPagination(items: List<T>, limit: Int?, offset: Int): List<T> {
        val end = if (limit != null) offset + limit else items.size
        return items.drop(offset).take(end - offset)
    }
}

/**
 * Minimal topic summary from registry.
 * Returned by: GET /registry/topics/summary
 */
data class RegistryTopicSummary(
    val name: TopicName,
    val owner: String,
    val presenceType: String,
    val labels: List<Label>
)

/**
 * Basic topic metadata from registry (no config data).
 * Returned by: GET /registry/topic/{name}/info
 */
data class TopicBasicInfo(
    val name: TopicName,
    val owner: String?,
    val description: String?,
    val labels: List<Label>,
    val producer: String?,
    val presence: Presence?,
    val resourceRequirements: ResourceRequirements?
)

/**
 * Topic configuration and properties from registry.
 * Returned by: GET /registry/topic/{name}/config
 */
data class RegistryTopicConfig(
    val name: TopicName,
    val properties: TopicProperties,
    val config: TopicConfigMap,
    val perClusterProperties: Map<KafkaClusterIdentifier, TopicProperties>,
    val perClusterConfigOverrides: Map<KafkaClusterIdentifier, TopicConfigMap>,
    val perTagProperties: Map<Tag, TopicProperties>,
    val perTagConfigOverrides: Map<Tag, TopicConfigMap>
)

/**
 * Topic presence configuration from registry.
 * Returned by: GET /registry/topic/{name}/presence
 */
data class RegistryTopicPresence(
    val name: TopicName,
    val presence: Presence
)

/**
 * Minimal cluster summary (identifier + tags).
 * Returned by: GET /registry/clusters/tags
 */
data class ClusterSummaryInfo(
    val identifier: KafkaClusterIdentifier,
    val tags: List<Tag>
)

/**
 * Cluster configuration from registry.
 * Returned by: GET /registry/cluster/{identifier}/config
 */
data class ClusterConfigurationInfo(
    val identifier: KafkaClusterIdentifier,
    val connectionString: String,
    val tags: List<Tag>,
    val sslEnabled: Boolean,
    val saslEnabled: Boolean,
    val profiles: List<String>
)

/**
 * Minimal topic status summary for scanning.
 * Returned by: GET /inspect/topics/status-summary
 */
data class TopicStatusSummary(
    val topicName: TopicName,
    val overallOk: Boolean,
    val clusterStatuses: List<ClusterStatusSummary>
)

data class ClusterStatusSummary(
    val clusterIdentifier: KafkaClusterIdentifier,
    val ok: Boolean,
    val issueCount: Int
)

/**
 * Ultra-minimal topic statuses (name + status types per cluster).
 * Returned by: GET /inspect/topics
 */
data class MinimalTopicStatuses(
    val topicName: TopicName,
    val topicDescription: TopicDescription?,
    val aggStatusFlags: StatusFlags,
    val statusPerClusters: List<MinimalTopicClusterStatus>
)

data class MinimalTopicClusterStatus(
    val status: MinimalTopicOnClusterInspectionResult,
    val clusterIdentifier: KafkaClusterIdentifier
)

data class MinimalTopicOnClusterInspectionResult(
    val types: List<TopicInspectionResultType>
)

/**
 * Topic status flags and types per cluster.
 * Returned by: GET /inspect/topic/{name}/status
 */
data class TopicStatusInfo(
    val topicName: TopicName,
    val aggStatusFlags: StatusFlags,
    val availableActions: List<AvailableAction>,
    val clusterStatuses: List<TopicClusterStatusInfo>
)

data class TopicClusterStatusInfo(
    val clusterIdentifier: KafkaClusterIdentifier,
    val flags: StatusFlags,
    val types: List<String>,
    val exists: Boolean?,
    val lastRefreshTime: Long
)

/**
 * Topic configuration issues from inspection (expected vs actual diffs).
 * Returned by: GET /inspect/topic/{name}/config
 */
data class TopicInspectConfigInfo(
    val topicName: TopicName,
    val clusterConfigIssues: List<ClusterConfigIssues>
)

data class ClusterConfigIssues(
    val clusterIdentifier: KafkaClusterIdentifier,
    val wrongValues: List<WrongValueAssertion>,
    val updateValues: List<WrongValueAssertion>,
    val ruleViolations: List<RuleViolationIssue>,
    val currentConfigRuleViolations: List<RuleViolationIssue>
)

/**
 * ACL rules affecting topic.
 * Returned by: GET /inspect/topic/{name}/acls
 */
data class TopicAclInfo(
    val topicName: TopicName,
    val affectingAclRules: List<KafkaAclRule>
)

/**
 * Partition assignments and rebalancing info.
 * Returned by: GET /inspect/topic/{name}/assignments
 */
data class TopicAssignmentInfo(
    val topicName: TopicName,
    val clusterAssignments: Map<KafkaClusterIdentifier, ClusterAssignmentDetails>
)

data class ClusterAssignmentDetails(
    val existingTopicInfo: ExistingTopicInfo,
    val assignmentsDisbalance: AssignmentsDisbalance?,
    val currentReAssignments: Map<Partition, TopicPartitionReAssignment>
)

/**
 * Resource usage and requirements.
 * Returned by: GET /inspect/topic/{name}/resources
 */
data class TopicResourceInfo(
    val topicName: TopicName,
    val resourceRequirements: ResourceRequirements?,
    val clusterResources: Map<KafkaClusterIdentifier, TopicResourceRequiredUsages>
)

/**
 * Cluster status overview.
 * Returned by: GET /inspect/clusters/all-statuses
 */
data class ClusterStatusOverview(
    val identifier: KafkaClusterIdentifier,
    val stateType: String,
    val ok: Boolean,
    val lastRefreshTime: Long
)

/**
 * Live cluster state information.
 * Returned by: GET /inspect/cluster/{identifier}/state
 */
data class ClusterStateInfo(
    val identifier: KafkaClusterIdentifier,
    val stateType: String,
    val clusterInfo: ClusterInfoMinimal?,
    val lastRefreshTime: Long
)

data class ClusterInfoMinimal(
    val nodeIds: List<Int>,
    val controllerId: Int?,
    val version: String?,
    val securityEnabled: Boolean
)

/**
 * Cluster broker counts.
 * Returned by: GET /inspect/cluster/{identifier}/stats
 */
data class ClusterStatisticsInfo(
    val identifier: KafkaClusterIdentifier,
    val brokerCount: Int,
    val onlineBrokerCount: Int
)

/**
 * Cluster total disk usage (combined only, no per-topic breakdown).
 * Returned by: GET /inspect/cluster/{id}/disk-usage
 */
data class ClusterTotalDiskUsage(
    val identifier: KafkaClusterIdentifier,
    val combined: BrokerDisk,
    val worstCurrentUsageLevel: UsageLevel,
    val worstPossibleUsageLevel: UsageLevel,
    val errors: List<String>
)

/**
 * Disk usage for a specific topic on a cluster.
 * Returned by: GET /inspect/cluster/{id}/disk-usage/{topicName}
 */
data class ClusterTopicDiskUsageResult(
    val identifier: KafkaClusterIdentifier,
    val topicName: TopicName,
    val diskUsage: OptionalValue<TopicClusterDiskUsage>
)

/**
 * Consumer group summary (no partition details).
 * Returned by: GET /inspect/consumer/{cluster}/{groupId}/summary
 */
data class ConsumerGroupSummary(
    val groupId: ConsumerGroupId,
    val clusterIdentifier: KafkaClusterIdentifier,
    val status: String,
    val lagAmount: Long?,
    val lagStatus: String,
    val partitionAssignor: String,
    val topics: List<TopicName>
)

/**
 * Consumer group lag information.
 * Returned by: GET /inspect/consumer/{cluster}/{groupId}/lag
 */
data class ConsumerGroupLagInfo(
    val groupId: ConsumerGroupId,
    val clusterIdentifier: KafkaClusterIdentifier,
    val totalLag: Long?,
    val lagStatus: String,
    val topicLags: Map<TopicName, Long?>
)

/**
 * Consumer group topic members with partition details.
 * Returned by: GET /inspect/consumer/{cluster}/{groupId}/topics
 */
data class ConsumerGroupTopicMembersInfo(
    val groupId: ConsumerGroupId,
    val clusterIdentifier: KafkaClusterIdentifier,
    val topicMembers: List<TopicMembers>
)

/**
 * Consumer group ACL rules.
 * Returned by: GET /inspect/consumer/{cluster}/{groupId}/acls
 */
data class ConsumerGroupAclInfo(
    val groupId: ConsumerGroupId,
    val clusterIdentifier: KafkaClusterIdentifier,
    val affectingAclRules: List<KafkaAclRule>
)
