package com.infobip.kafkistry.api

import com.infobip.kafkistry.appinfo.ModulesBuildInfoLoader
import com.infobip.kafkistry.model.*
import com.infobip.kafkistry.service.OptionalValue
import com.infobip.kafkistry.service.acl.AclsRegistryService
import com.infobip.kafkistry.service.background.BackgroundJobIssuesRegistry
import com.infobip.kafkistry.service.cluster.ClustersRegistryService
import com.infobip.kafkistry.service.cluster.ClusterStatusService
import com.infobip.kafkistry.service.consumers.AllConsumersData
import com.infobip.kafkistry.service.consumers.ClusterConsumerGroup
import com.infobip.kafkistry.service.consumers.ConsumersService
import com.infobip.kafkistry.service.consumers.KafkaConsumerGroup
import com.infobip.kafkistry.service.history.HistoryService
import com.infobip.kafkistry.service.quotas.QuotasRegistryService
import com.infobip.kafkistry.service.resources.ClusterResourcesAnalyzer
import com.infobip.kafkistry.service.resources.TopicResourcesAnalyzer
import com.infobip.kafkistry.service.scrapingstatus.ScrapingStatusService
import com.infobip.kafkistry.service.topic.*
import com.infobip.kafkistry.sql.SQLRepository
import org.springframework.web.bind.annotation.*

/**
 * MCP-optimized API endpoints for Claude Code MCP integration.
 * These endpoints wrap existing service methods and apply server-side filtering
 * to reduce token usage while keeping original APIs unchanged.
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
     * List all topics with inspection status, optimized for MCP.
     * Filters out heavy objects: configEntryStatuses, currentTopicReplicaInfos, resourceRequiredUsages.
     *
     * @param limit Maximum number of topics to return
     * @param offset Number of topics to skip
     * @param clustersOnly Comma-separated cluster identifiers to filter by
     * @return List of topic statuses with filtered data
     */
    @GetMapping("/inspect/topics")
    fun inspectTopicsOptimized(
        @RequestParam("limit", required = false) limit: Int?,
        @RequestParam("offset", required = false) offset: Int?,
        @RequestParam("clustersOnly", required = false) clustersOnly: String?
    ): List<TopicStatuses> {
        // Call existing service (unchanged)
        val allTopics = topicsInspectionService.inspectAllTopics()

        // Apply pagination first (more efficient)
        val paginated = applyPagination(allTopics, limit, offset)

        // Apply cluster filtering if needed
        val filtered = if (clustersOnly != null) {
            val clusterIds = clustersOnly.split(",").toSet()
            paginated.map { topic ->
                topic.copy(
                    statusPerClusters = topic.statusPerClusters.filter {
                        it.clusterIdentifier in clusterIds
                    }
                )
            }
        } else {
            paginated
        }

        // Apply field filtering (remove heavy objects)
        return filtered.map { topic ->
            topic.copy(
                statusPerClusters = topic.statusPerClusters.map { cluster ->
                    cluster.copy(
                        configEntryStatuses = null,
                        currentTopicReplicaInfos = null,
                        resourceRequiredUsages = OptionalValue.absent("Excluded for MCP")
                    )
                }
            )
        }
    }

    /**
     * Inspect a specific topic, optionally including full details.
     *
     * @param topicName Name of the topic to inspect
     * @param includeDetails If false, filters out heavy objects (default: false)
     * @return Topic status with optional filtering
     */
    @GetMapping("/inspect/topic")
    fun inspectTopicOptimized(
        @RequestParam("topicName") topicName: TopicName,
        @RequestParam("includeDetails", required = false, defaultValue = "false") includeDetails: Boolean
    ): TopicStatuses {
        val topicStatus = topicsInspectionService.inspectTopic(topicName)

        return if (includeDetails) {
            topicStatus
        } else {
            // Filter out heavy objects
            topicStatus.copy(
                statusPerClusters = topicStatus.statusPerClusters.map { cluster ->
                    cluster.copy(
                        configEntryStatuses = null,
                        currentTopicReplicaInfos = null,
                        resourceRequiredUsages = OptionalValue.absent("Excluded for MCP")
                    )
                }
            )
        }
    }

    /**
     * List all topics from registry, with optional filtering to names-only.
     *
     * @param limit Maximum number of topics to return
     * @param offset Number of topics to skip
     * @param namesOnly If true, returns only topic names (default: false)
     * @return List of topics or topic names
     */
    @GetMapping("/topics")
    fun listTopicsOptimized(
        @RequestParam("limit", required = false) limit: Int?,
        @RequestParam("offset", required = false) offset: Int?,
        @RequestParam("namesOnly", required = false, defaultValue = "false") namesOnly: Boolean
    ): Any {
        val allTopics = topicsRegistryService.listTopics()
        val paginated = applyPagination(allTopics, limit, offset)

        return if (namesOnly) {
            paginated.map { it.name }
        } else {
            paginated
        }
    }

    /**
     * Ultra-minimal endpoint: Get only topic names.
     * Provides maximum token savings (~90% reduction).
     *
     * @param limit Maximum number of names to return
     * @param offset Number of names to skip
     * @return List of topic names only
     */
    @GetMapping("/topics/names")
    fun listTopicNamesOnly(
        @RequestParam("limit", required = false) limit: Int?,
        @RequestParam("offset", required = false) offset: Int?
    ): List<TopicName> {
        val allTopics = topicsRegistryService.listTopics()
        val paginated = applyPagination(allTopics, limit, offset)
        return paginated.map { it.name }
    }

    /**
     * Ultra-minimal endpoint: Get topic status summary (name + OK status + issue count per cluster).
     * Provides significant token savings (~85% reduction).
     *
     * @param limit Maximum number of topics to return
     * @param offset Number of topics to skip
     * @param clustersOnly Comma-separated cluster identifiers to filter by
     * @return List of minimal status summaries
     */
    @GetMapping("/topics/status-summary")
    fun listTopicStatusSummary(
        @RequestParam("limit", required = false) limit: Int?,
        @RequestParam("offset", required = false) offset: Int?,
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
     * List all consumer groups across all clusters, optimized for MCP.
     * Filters out heavy objects: partitionAssignor, topicMembers partition details.
     *
     * @param limit Maximum number of consumer groups to return
     * @param clusterFilter Comma-separated cluster identifiers to filter by
     * @param includePartitionDetails If false, removes partition details (default: false)
     * @return Consumer groups data with optional filtering
     */
    @GetMapping("/consumers")
    fun listConsumersOptimized(
        @RequestParam("limit", required = false) limit: Int?,
        @RequestParam("clusterFilter", required = false) clusterFilter: String?,
        @RequestParam("includePartitionDetails", required = false, defaultValue = "false") includePartitionDetails: Boolean
    ): AllConsumersData {
        // Call existing service (unchanged)
        val consumersData = consumersService.allConsumersData()

        // Apply cluster filtering if specified
        val clusterIdentifiers = clusterFilter?.split(",")?.toSet()
        val filteredClusters = if (clusterIdentifiers != null) {
            consumersData.clustersGroups.filter { it.clusterIdentifier in clusterIdentifiers }
        } else {
            consumersData.clustersGroups
        }

        // Apply limit if specified
        val limitedClusters = if (limit != null) {
            filteredClusters.take(limit)
        } else {
            filteredClusters
        }

        // Apply field filtering if partition details not needed
        val optimizedClusters = if (includePartitionDetails) {
            limitedClusters
        } else {
            limitedClusters.map { clusterGroup ->
                clusterGroup.copy(
                    consumerGroup = clusterGroup.consumerGroup.copy(
                        partitionAssignor = "",  // Remove partition assignor
                        topicMembers = clusterGroup.consumerGroup.topicMembers.map { topicMember ->
                            topicMember.copy(
                                partitionMembers = emptyList()  // Remove partition details
                            )
                        }
                    )
                )
            }
        }

        return AllConsumersData(
            clustersDataStatuses = consumersData.clustersDataStatuses,
            clustersGroups = optimizedClusters
        )
    }

    /**
     * Get a specific consumer group, optimized for MCP.
     *
     * @param clusterIdentifier Cluster identifier
     * @param consumerGroupId Consumer group ID
     * @param includePartitionDetails If false, removes partition details (default: false)
     * @return Consumer group with optional filtering
     */
    @GetMapping("/consumers/cluster/{clusterIdentifier}/group/{consumerGroupId}")
    fun getConsumerGroupOptimized(
        @PathVariable("clusterIdentifier") clusterIdentifier: KafkaClusterIdentifier,
        @PathVariable("consumerGroupId") consumerGroupId: ConsumerGroupId,
        @RequestParam("includePartitionDetails", required = false, defaultValue = "false") includePartitionDetails: Boolean
    ): KafkaConsumerGroup? {
        val consumerGroup = consumersService.consumerGroup(clusterIdentifier, consumerGroupId)

        return if (consumerGroup == null || includePartitionDetails) {
            consumerGroup
        } else {
            // Filter out partition details
            consumerGroup.copy(
                partitionAssignor = "",  // Remove partition assignor
                topicMembers = consumerGroup.topicMembers.map { topicMember ->
                    topicMember.copy(
                        partitionMembers = emptyList()  // Remove partition details
                    )
                }
            )
        }
    }

    // ==================== CLUSTER ENDPOINTS ====================

    /**
     * List all Kafka clusters.
     *
     * @return List of Kafka clusters
     */
    @GetMapping("/clusters")
    fun listClusters(): List<KafkaCluster> {
        return clustersRegistryService.listClusters()
    }

    /**
     * Get details for a specific cluster.
     *
     * @param clusterIdentifier Cluster identifier
     * @return Cluster configuration
     */
    @GetMapping("/clusters/{clusterIdentifier}")
    fun getCluster(
        @PathVariable("clusterIdentifier") clusterIdentifier: KafkaClusterIdentifier
    ): KafkaCluster {
        return clustersRegistryService.getCluster(clusterIdentifier)
    }

    /**
     * Inspect status of all clusters.
     *
     * @return List of cluster statuses
     */
    @GetMapping("/inspect/clusters")
    fun inspectClusters() = clusterStatusService.clustersState()

    /**
     * Get disk usage statistics for a cluster.
     *
     * @param clusterIdentifier Cluster identifier
     * @return Cluster disk usage data
     */
    @GetMapping("/clusters/{clusterIdentifier}/disk-usage")
    fun getClusterDiskUsage(
        @PathVariable("clusterIdentifier") clusterIdentifier: KafkaClusterIdentifier
    ) = clusterResourcesAnalyzer.clusterDiskUsage(clusterIdentifier)

    // ==================== SECURITY ENDPOINTS (ACLs & Quotas) ====================

    /**
     * List all ACLs (Access Control Lists).
     *
     * @return List of principal ACL rules
     */
    @GetMapping("/acls")
    fun listAcls(): List<PrincipalAclRules> {
        return aclsRegistryService.listAllPrincipalsAcls()
    }

    /**
     * Get ACLs for a specific principal.
     *
     * @param principal Principal identifier
     * @return Principal ACL rules
     */
    @GetMapping("/acls/{principal}")
    fun getPrincipalAcls(
        @PathVariable("principal") principal: PrincipalId
    ): PrincipalAclRules {
        return aclsRegistryService.getPrincipalAcls(principal)
    }

    /**
     * List all client quotas.
     *
     * @return List of quota descriptions
     */
    @GetMapping("/quotas")
    fun listQuotas(): List<QuotaDescription> {
        return quotasRegistryService.listAllQuotas()
    }

    /**
     * Get quotas for a specific entity.
     *
     * @param quotaEntityID Quota entity identifier
     * @return Quota description
     */
    @GetMapping("/quotas/{quotaEntityID}")
    fun getQuotas(
        @PathVariable("quotaEntityID") quotaEntityID: QuotaEntityID
    ): QuotaDescription {
        return quotasRegistryService.getQuotas(quotaEntityID)
    }

    // ==================== SYSTEM ENDPOINTS ====================

    /**
     * Get Kafkistry build and version information.
     *
     * @return Build information
     */
    @GetMapping("/build-info")
    fun getBuildInfo() = buildInfoLoader.modulesInfos()

    /**
     * Get current background job issues and status.
     *
     * @return Background job issues
     */
    @GetMapping("/background-issues")
    fun getBackgroundIssues() = backgroundIssuesRegistry.currentIssues()

    /**
     * Get cluster scraping status.
     *
     * @return Scraping status for all clusters
     */
    @GetMapping("/scraping-status")
    fun getScrapingStatus() = scrapingStatusService.currentScrapingStatuses()

    /**
     * Get recent change history.
     *
     * @param count Number of recent changes to retrieve (default: 10)
     * @return Recent change history
     */
    @GetMapping("/history")
    fun getHistory(
        @RequestParam("count", required = false, defaultValue = "10") count: Int
    ) = historyService.getRecentHistory(count)

    // ==================== ANALYSIS ENDPOINTS ====================

    /**
     * Execute SQL query on Kafka metadata.
     *
     * @param sql SQL query string
     * @return Query results
     */
    @PostMapping("/sql")
    fun executeSqlQuery(
        @RequestParam("sql") sql: String
    ) = sqlRepository.query(sql)

    /**
     * Get disk usage for a specific topic on a cluster.
     *
     * @param topicName Topic name
     * @param clusterIdentifier Cluster identifier
     * @return Topic disk usage data
     */
    @GetMapping("/topics/{topicName}/clusters/{clusterIdentifier}/disk-usage")
    fun getTopicDiskUsage(
        @PathVariable("topicName") topicName: TopicName,
        @PathVariable("clusterIdentifier") clusterIdentifier: KafkaClusterIdentifier
    ) = topicResourcesAnalyzer.topicOnClusterDiskUsage(topicName, clusterIdentifier)

    /**
     * Generate configuration suggestion for importing an unknown topic.
     *
     * @param topicName Topic name to import
     * @return Topic import suggestions
     */
    @GetMapping("/topics/{topicName}/import-suggestion")
    fun suggestTopicImport(
        @PathVariable("topicName") topicName: TopicName
    ) = suggestionService.suggestTopicImport(topicName)

    /**
     * Apply pagination to a list.
     */
    private fun <T> applyPagination(items: List<T>, limit: Int?, offset: Int?): List<T> {
        val start = offset ?: 0
        val end = if (limit != null) start + limit else items.size
        return items.drop(start).take(end - start)
    }
}

/**
 * Minimal topic status summary for MCP optimization.
 */
data class TopicStatusSummary(
    val topicName: TopicName,
    val overallOk: Boolean,
    val clusterStatuses: List<ClusterStatusSummary>
)

/**
 * Minimal cluster status for a topic.
 */
data class ClusterStatusSummary(
    val clusterIdentifier: KafkaClusterIdentifier,
    val ok: Boolean,
    val issueCount: Int
)

