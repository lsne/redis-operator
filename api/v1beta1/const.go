// Created by lsne on 2022-11-06 21:38:44

package v1beta1

import (
	"time"
)

// 标签相关
const (
	LabelPrefix string = "mysite.cn/"

	RedisReplicaFinalizer string = LabelPrefix + "redisReplicaFinalizer"
	RedisClusterFinalizer string = LabelPrefix + "redisClusterFinalizer"

	ControllerNameKey   string = LabelPrefix + "ctrlname"
	ControllerNameValue string = "redis-operator"

	ClusterKindKey   string = LabelPrefix + "clusterKind"
	RedisReplicaKind string = "RedisReplica"
	RedisClusterKind string = "RedisCluster"

	RedisRoleKey    string = "redisRole"
	RedisMasterRole string = "master"
	RedisSlaveRole  string = "slave"

	ClusterNameKey     string = "clusterName"
	StatefulSetNameKey string = "statefulSetName"
)

const (
	RedisReplicaPrefix string = "myrr-"
	RedisClusterPrefix string = "myrc-"
)

const (
	DefaultHashMaxSlots = 16383
)

const (
	PodDeleteWait = time.Duration(5 * time.Minute)
)

const (
	DefaultRedisImage           string = "redis:6.0.16-alpine"
	DefaultRedisImagePullPolicy string = "IfNotPresent"
)

const (
	RedisBinVolumeName    = "redis-bin"
	RedisConfigVolumeName = "redis-conf"
	RedisDataVolumeName   = "redis-data"
	RedisCertVolumeName   = "redis-cert"
	RedisLabelsVolumeName = "redis-labels"

	RedisBinVolumeMountPath         = "/redis/bin"
	RedisConfigVolumeMountPath      = "/redis/config"
	RedisDataVolumeMountPath        = "/redis/data"
	RedisCertVolumeMountPath        = "/redis/cert"
	RedisLabelsVolumeMountPath      = "/redis/labels"
	RedisLabelsVolumeMountFilename  = "labels"
	RedisLabelsVolumeMountFieldname = "metadata.labels"
)

const (
	RedisHealthCheckInitialDelaySeconds int32  = 30
	RedisHealthCheckTimeoutSeconds      int32  = 5
	probeArg                            string = "redis-cli -h $(hostname) ping"
)

const (
	RedisConfigMapNameSuffix = "-rcm"
	RedisStartupScript       = "redis_startup.sh"
	RedisShutdownScript      = "redis_shutdown.sh"
	RedisHealthCheckScript   = "redis_check.sh"
)

const (
	RedisStartupScriptContent = `#!/bin/sh

set -e

REDIS_BASE_PATH="/redis"
REDIS_DATA_PATH="${REDIS_BASE_PATH}/data"
REDIS_CONFIG_PATH="${REDIS_BASE_PATH}/config"
REDIS_CERT_PATH="${REDIS_BASE_PATH}/cert"

REDIS_CONFIG_FILE="${REDIS_CONFIG_PATH}/redis.conf"
REDIS_NODE_FILE="${REDIS_DATA_PATH}/node.conf"

init_redis_path() {
	mkdir -p ${REDIS_BASE_PATH}
	mkdir -p ${REDIS_DATA_PATH}
	mkdir -p ${REDIS_CONFIG_PATH}
	mkdir -p ${REDIS_CERT_PATH}
}

init_redis_config_tls() {
	echo ""
	echo "tls-port ${REDIS_CONFIG_TLS_PORT}"
	echo "tls-auth-clients yes"
	echo "tls-replication yes"
	echo "tls-cluster yes"
	[ -f "${REDIS_CERT_PATH}/redis.crt" ] && echo "tls-cert-file ${REDIS_CERT_PATH}/redis.crt"
	[ -f "${REDIS_CERT_PATH}/redis.key" ] && echo "tls-key-file ${REDIS_CERT_PATH}/redis.key"
	[ -f "${REDIS_CERT_PATH}/ca.crt" ] && echo "tls-ca-cert-file ${REDIS_CERT_PATH}/ca.crt"
	[ -f "${REDIS_CERT_PATH}/redis.dh" ] && echo "tls-dh-params-file ${REDIS_CERT_PATH}/redis.dh"
}

init_redis_config_user() {
	echo ""
	[ -z "${REDIS_ROOT_USERNAME}" ] && REDIS_ROOT_USERNAME="default"

	if [ -z "${REDIS_REPLICATION_USERNAME}" ] || [ -z "${REDIS_REPLICATION_PASSWORD}" ]; then
		[ -z "${REDIS_ROOT_USERNAME}" ] || echo "masteruser ${REDIS_ROOT_USERNAME}"
		[ -z "${REDIS_ROOT_PASSWORD}" ] || echo "masterauth ${REDIS_ROOT_PASSWORD}"
	else
		echo "user ${REDIS_REPLICATION_USERNAME} on #$(printf "%s" "${REDIS_REPLICATION_PASSWORD}" | sha256sum | awk '{print $1}') -@all +psync +replconf +ping"
		echo "masteruser ${REDIS_REPLICATION_USERNAME}"
		echo "masterauth ${REDIS_REPLICATION_PASSWORD}"
	fi

	if [ "${REDIS_ROOT_USERNAME}" = "default" ]; then
		[ -z "${REDIS_ROOT_PASSWORD}" ] || echo "user default on #$(printf "%s" "${REDIS_ROOT_PASSWORD}" | sha256sum | awk '{print $1}') allkeys +@all"
	else
		echo "user default off"
		[ -z "${REDIS_ROOT_PASSWORD}" ] || echo "user ${REDIS_ROOT_USERNAME} on #$(printf "%s" "${REDIS_ROOT_PASSWORD}" | sha256sum | awk '{print $1}') allkeys +@all"
	fi

	if [ -n "${REDIS_MONITOR_USERNAME}" ] || [ -n "${REDIS_MONITOR_PASSWORD}" ]; then
		echo "user ${REDIS_MONITOR_USERNAME} on #$(printf "%s" "${REDIS_MONITOR_PASSWORD}" | sha256sum | awk '{print $1}') +client +ping +info +config|get +cluster|info +slowlog +latency +memory +select +get +scan +xinfo +type +pfcount +strlen +llen +scard +zcard +hlen +xlen +eval allkeys"
	fi

	if [ -n "${REDIS_APP01_USERNAME}" ] || [ -n "${REDIS_APP01_PASSWORD}" ]; then
		echo "user ${REDIS_APP01_USERNAME} on #$(printf "%s" "${REDIS_APP01_PASSWORD}" | sha256sum | awk '{print $1}') ${REDIS_APP01_PRIVILEGES}"
	fi

	if [ -n "${REDIS_APP02_USERNAME}" ] || [ -n "${REDIS_APP02_PASSWORD}" ]; then
		echo "user ${REDIS_APP02_USERNAME} on #$(printf "%s" "${REDIS_APP02_PASSWORD}" | sha256sum | awk '{print $1}') ${REDIS_APP02_PRIVILEGES}"
	fi

	if [ -n "${REDIS_APP03_USERNAME}" ] || [ -n "${REDIS_APP03_PASSWORD}" ]; then
		echo "user ${REDIS_APP03_USERNAME} on #$(printf "%s" "${REDIS_APP03_PASSWORD}" | sha256sum | awk '{print $1}') ${REDIS_APP03_PRIVILEGES}"
	fi
}

init_redis_config() {
	: >${REDIS_CONFIG_FILE}
	{
		[ -z "${REDIS_CUSTOM_CONFIG}" ] || echo "${REDIS_CUSTOM_CONFIG}"

		echo ""
		[ -z "${REDIS_CONFIG_ACTIVEREHASHING}" ] || echo "activerehashing ${REDIS_CONFIG_ACTIVEREHASHING}"
		[ -z "${REDIS_CONFIG_ACTIVEDEFRAG}" ] || echo "activedefrag ${REDIS_CONFIG_ACTIVEDEFRAG}"
		[ -z "${REDIS_CONFIG_ACTIVE_DEFRAG_CYCLE_MIN}" ] || echo "active-defrag-cycle-min ${REDIS_CONFIG_ACTIVE_DEFRAG_CYCLE_MIN}"
		[ -z "${REDIS_CONFIG_ACTIVE_DEFRAG_CYCLE_MAX}" ] || echo "active-defrag-cycle-max ${REDIS_CONFIG_ACTIVE_DEFRAG_CYCLE_MAX}"
		[ -z "${REDIS_CONFIG_ACTIVE_DEFRAG_IGNORE_BYTES}" ] || echo "active-defrag-ignore-bytes ${REDIS_CONFIG_ACTIVE_DEFRAG_IGNORE_BYTES}"
		[ -z "${REDIS_CONFIG_ACTIVE_DEFRAG_THRESHOLD_LOWER}" ] || echo "active-defrag-threshold-lower ${REDIS_CONFIG_ACTIVE_DEFRAG_THRESHOLD_LOWER}"
		[ -z "${REDIS_CONFIG_ACTIVE_DEFRAG_THRESHOLD_UPPER}" ] || echo "active-defrag-threshold-upper ${REDIS_CONFIG_ACTIVE_DEFRAG_THRESHOLD_UPPER}"

		echo ""
		[ -z "${REDIS_CONFIG_APPENDONLY}" ] || echo "appendonly ${REDIS_CONFIG_APPENDONLY}"
		[ -z "${REDIS_CONFIG_APPENDFSYNC}" ] || echo "appendfsync ${REDIS_CONFIG_APPENDFSYNC}"
		[ -z "${REDIS_CONFIG_AOF_LOAD_TRUNCATED}" ] || echo "aof-load-truncated ${REDIS_CONFIG_AOF_LOAD_TRUNCATED}"
		[ -z "${REDIS_CONFIG_AOF_USE_RDB_PREAMBLE}" ] || echo "aof-use-rdb-preamble ${REDIS_CONFIG_AOF_USE_RDB_PREAMBLE}"
		[ -z "${REDIS_CONFIG_NO_APPENDFSYNC_ON_REWRITE}" ] || echo "no-appendfsync-on-rewrite ${REDIS_CONFIG_NO_APPENDFSYNC_ON_REWRITE}"
		[ -z "${REDIS_CONFIG_AUTO_AOF_REWRITE_PERCENTAGE}" ] || echo "auto-aof-rewrite-percentage ${REDIS_CONFIG_AUTO_AOF_REWRITE_PERCENTAGE}"
		[ -z "${REDIS_CONFIG_AUTO_AOF_REWRITE_MIN_SIZE}" ] || echo "auto-aof-rewrite-min-size ${REDIS_CONFIG_AUTO_AOF_REWRITE_MIN_SIZE}"
		echo "appendfilename \"appendonly.aof\""

		echo ""
		echo "bind 0.0.0.0"
		[ -z "${REDIS_CONFIG_PROTECTED_MODE}" ] || echo "protected-mode ${REDIS_CONFIG_PROTECTED_MODE}"
		[ -z "${REDIS_CONFIG_PROTECTED_MODE}" ] || echo "tcp-backlog ${REDIS_CONFIG_PROTECTED_MODE}"
		[ -z "${REDIS_CONFIG_PROTECTED_MODE}" ] || echo "tcp-keepalive ${REDIS_CONFIG_PROTECTED_MODE}"
		[ -z "${REDIS_CONFIG_LOGLEVEL}" ] || echo "loglevel ${REDIS_CONFIG_LOGLEVEL}"
		[ -z "${REDIS_CONFIG_PORT}" ] || echo "port ${REDIS_CONFIG_PORT}"
		[ -z "${REDIS_CONFIG_DATABASES}" ] || echo "databases ${REDIS_CONFIG_DATABASES}"
		[ -z "${REDIS_CONFIG_DIR}" ] || echo "dir ${REDIS_DATA_PATH}"
		echo "dbfilename dump.rdb"
		[ -z "${REDIS_CONFIG_RDBCHECKSUM}" ] || echo "rdbchecksum ${REDIS_CONFIG_RDBCHECKSUM}"
		[ -z "${REDIS_CONFIG_RDBCOMPRESSION}" ] || echo "rdbcompression ${REDIS_CONFIG_RDBCOMPRESSION}"
		[ -z "${REDIS_CONFIG_STOP_WRITES_ON_BGSAVE_ERROR}" ] || echo "stop-writes-on-bgsave-error ${REDIS_CONFIG_STOP_WRITES_ON_BGSAVE_ERROR}"
		[ -z "${REDIS_CONFIG_TIMEOUT}" ] || echo "timeout ${REDIS_CONFIG_TIMEOUT}"
		[ -z "${REDIS_CONFIG_MAXCLIENTS}" ] || echo "maxclients ${REDIS_CONFIG_MAXCLIENTS}"
		[ -z "${REDIS_CONFIG_MAXMEMORY}" ] || echo "maxmemory ${REDIS_CONFIG_MAXMEMORY}"
		[ -z "${REDIS_CONFIG_MAXMEMORY_POLICY}" ] || echo "maxmemory-policy ${REDIS_CONFIG_MAXMEMORY_POLICY}"
		[ -z "${REDIS_CONFIG_MAXMEMORY_SAMPLES}" ] || echo "maxmemory-samples ${REDIS_CONFIG_MAXMEMORY_SAMPLES}"
		[ -z "${REDIS_CONFIG_HZ}" ] || echo "hz ${REDIS_CONFIG_HZ}"
		[ -z "${REDIS_CONFIG_REPL_BACKLOG_SIZE}" ] || echo "repl-backlog-size ${REDIS_CONFIG_REPL_BACKLOG_SIZE}"
		[ -z "${REDIS_CONFIG_REPL_BACKLOG_TTL}" ] || echo "repl-backlog-ttl ${REDIS_CONFIG_REPL_BACKLOG_TTL}"
		[ -z "${REDIS_CONFIG_SLOWLOG_LOG_SLOWER_THAN}" ] || echo "slowlog-log-slower-than ${REDIS_CONFIG_SLOWLOG_LOG_SLOWER_THAN}"
		[ -z "${REDIS_CONFIG_SLOWLOG_MAX_LEN}" ] || echo "slowlog-max-len ${REDIS_CONFIG_SLOWLOG_MAX_LEN}"

		[ -z "${REDIS_CONFIG_TLS_PORT}" ] || [ "${REDIS_CONFIG_TLS_PORT}" -eq 0 ] || init_redis_config_tls

		[ -z "${REDIS_CONFIG_CLUSTER_ENABLED}" ] || echo "cluster-enabled ${REDIS_CONFIG_CLUSTER_ENABLED}"
		echo "cluster-config-file ${REDIS_NODE_FILE}"
		[ -z "${REDIS_CONFIG_CLUSTER_MIGRATION_BARRIER}" ] || echo "cluster-migration-barrier ${REDIS_CONFIG_CLUSTER_MIGRATION_BARRIER}"
		[ -z "${REDIS_CONFIG_CLUSTER_REQUIRE_FULL_COVERAGE}" ] || echo "cluster-require-full-coverage ${REDIS_CONFIG_CLUSTER_REQUIRE_FULL_COVERAGE}"
		[ -z "${REDIS_CONFIG_CLUSTER_NODE_TIMEOUT}" ] || echo "cluster-node-timeout ${REDIS_CONFIG_CLUSTER_NODE_TIMEOUT}"

		init_redis_config_user

		[ -z "${REDIS_CONFIG_SET_MAX_INTSET_ENTRIES}" ] || echo "set-max-intset-entries ${REDIS_CONFIG_SET_MAX_INTSET_ENTRIES}"
		[ -z "${REDIS_CONFIG_HASH_MAX_ZIPLIST_ENTRIES}" ] || echo "hash-max-ziplist-entries ${REDIS_CONFIG_HASH_MAX_ZIPLIST_ENTRIES}"
		[ -z "${REDIS_CONFIG_HASH_MAX_ZIPLIST_VALUE}" ] || echo "hash-max-ziplist-value ${REDIS_CONFIG_HASH_MAX_ZIPLIST_VALUE}"
		[ -z "${REDIS_CONFIG_LIST_MAX_ZIPLIST_ENTRIES}" ] || echo "list-max-ziplist-entries ${REDIS_CONFIG_LIST_MAX_ZIPLIST_ENTRIES}"
		[ -z "${REDIS_CONFIG_LIST_MAX_ZIPLIST_VALUE}" ] || echo "list-max-ziplist-value ${REDIS_CONFIG_LIST_MAX_ZIPLIST_VALUE}"
		[ -z "${REDIS_CONFIG_ZSET_MAX_ZIPLIST_ENTRIES}" ] || echo "zset-max-ziplist-entries ${REDIS_CONFIG_ZSET_MAX_ZIPLIST_ENTRIES}"
		[ -z "${REDIS_CONFIG_ZSET_MAX_ZIPLIST_VALUE}" ] || echo "zset-max-ziplist-value ${REDIS_CONFIG_ZSET_MAX_ZIPLIST_VALUE}"

		echo ""
		[ -z "${REDIS_CONFIG_CLIENT_OUTPUT_BUFFER_LIMIT}" ] || echo "${REDIS_CONFIG_CLIENT_OUTPUT_BUFFER_LIMIT}"
		[ -z "${REDIS_CONFIG_SAVE}" ] || echo "${REDIS_CONFIG_SAVE}"
		[ -z "${REDIS_CONFIG_RENAME_COMMAND}" ] || echo "${REDIS_CONFIG_RENAME_COMMAND}"
		echo "daemonize no"
		echo "supervised no"

	} >>${REDIS_CONFIG_FILE}
}

fix_node_config() {
	if [ -f ${REDIS_NODE_FILE} ]; then
		if [ -z "${POD_IP}" ]; then
			POD_IP=$(hostname -i)
		fi
		echo "Updating my IP to ${POD_IP} in ${CLUSTER_CONFIG}"
		sed -i.bak -e "/myself/ s/ .*:6379@16379/ ${POD_IP}:6379@16379/" ${CLUSTER_CONFIG}
	fi
}

main() {
	init_redis_path
	init_redis_config
	fix_node_config
	redis-server ${REDIS_CONFIG_FILE}
}

main
`

	RedisShutdownScriptContent = `#!/bin/sh
CLUSTER_CONFIG="/redis/data/nodes.conf"
redis_cli_args=""
authinfo=""
tlsinfo=""
[ -z "${REDIS_ROOT_USERNAME}" ] && REDIS_ROOT_USERNAME="default"
[ -z "${REDIS_ROOT_PASSWORD}" ] && authinfo="--user=${REDIS_ROOT_USERNAME} --pass="${REDIS_ROOT_PASSWORD}"
[ -f "/redis/cert/redis.key" ] && tlsinfo="--tls --cert /redis/cert/redis.crt --key /redis/cert/redis.key --cacert /redis/cert/ca.crt"

failover() {
	echo "Do CLUSTER FAILOVER"
	masterID=$(cat ${CLUSTER_CONFIG} | grep "myself" | awk '{print $1}')
	echo "Master: ${masterID}"
	slave=$(cat ${CLUSTER_CONFIG} | grep ${masterID} | grep "slave" | awk 'NR==1{print $2}' | sed "s/:${REDIS_CONFIG_PORT}@1${REDIS_CONFIG_PORT}//")
	echo "Slave: ${slave}"

	redis-cli -h ${slave} ${authinfo} ${tlsinfo} CLUSTER FAILOVER

	echo "Wait for MASTER <-> SLAVE syncFinished"
	sleep 20
}

if [ -f ${CLUSTER_CONFIG} ]; then
	cat ${CLUSTER_CONFIG} | grep "myself" | grep "master" && failover
fi
`

	RedisHealthCheckScriptContent = `#!/bin/bash
check_redis_health() {
    if [ -z "${REDIS_CONFIG_TLS_PORT}" ] || [ "${REDIS_CONFIG_TLS_PORT}" -eq 0 ]; then
        redis-cli -p ${REDIS_CONFIG_PORT} ping
    else
        redis-cli -p ${REDIS_CONFIG_TLS_PORT} --tls --cert /redis/cert/redis.crt --key /redis/cert/redis.key --cacert /redis/cert/ca.crt ping 
    fi
}

check_redis_health
`
)
