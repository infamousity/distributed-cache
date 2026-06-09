#!/usr/bin/env bash
set -euo pipefail

# Chaos/proof harness for the example Swarm stack.
#
# Scope:
#   This script is intentionally tied to examples/swarm/docker-stack.yml and the
#   example app service. It proves the basic runtime contract for the example:
#   tasks discover peers through DNS, form a memberlist ring, continue reading the
#   replicated key across scale churn, accept active set/get/delete commands
#   through the example harness API, and survive a forced rolling update.
#
# Non-goals:
#   This is not a generic production validation tool. It proves ordinary delete
#   visibility, but it does not yet prove tombstone safety under deliberately
#   injected stale older-version writes.
#
# Docker context:
#   Every docker command is issued with an explicit --context. Set DOCKER_CONTEXT
#   when targeting a non-default Swarm manager.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

DOCKER_CONTEXT="${DOCKER_CONTEXT:-default}"
STACK_NAME="${STACK_NAME:-example}"
SERVICE_NAME="${STACK_NAME}_app"
NETWORK_NAME="${STACK_NAME}_cache_control"
IMAGE="${IMAGE:-distributed-cache-example-app:latest}"
REPLICAS="${REPLICAS:-3}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
STEADY_SECONDS="${STEADY_SECONDS:-15}"
DEBUG_LOG_LINES="${DEBUG_LOG_LINES:-80}"
PLACEMENT_CONSTRAINT="${PLACEMENT_CONSTRAINT:-node.platform.os == linux}"
HARNESS_URL="${HARNESS_URL:-${APP_URL:-}}"
HARNESS_TRANSPORT="${HARNESS_TRANSPORT:-host}"
HARNESS_HOST_PORT="${HARNESS_HOST_PORT:-18080}"
GOSSIP_DEGRADATION_MODE="${GOSSIP_DEGRADATION_MODE:-warn}"
export PLACEMENT_CONSTRAINT

docker_cmd() {
	docker --context "${DOCKER_CONTEXT}" "$@"
}

log() {
	printf '[swarm-chaos] %s\n' "$*"
}

fail() {
	printf '[swarm-chaos] ERROR: %s\n' "$*" >&2
	print_debug || true
	exit 1
}

print_debug() {
	log "service status:"
	docker_cmd service ls --filter "name=${SERVICE_NAME}" || true
	log "recent tasks:"
	docker_cmd service ps "${SERVICE_NAME}" --no-trunc || true
	log "harness status:"
	harness_request GET "/status" || true
	printf '\n'
	log "recent logs:"
	docker_cmd service logs --raw --tail "${DEBUG_LOG_LINES}" "${SERVICE_NAME}" || true
}

require_swarm() {
	if [[ "${HARNESS_TRANSPORT}" == "host" || -n "${HARNESS_URL}" ]] && ! command -v curl >/dev/null 2>&1; then
		fail "curl is required for harness HTTP assertions"
	fi
	if [[ "${HARNESS_TRANSPORT}" == "ssh" || "${HARNESS_TRANSPORT}" == "ssh-exec" ]] && ! command -v ssh >/dev/null 2>&1; then
		fail "ssh is required when HARNESS_TRANSPORT=${HARNESS_TRANSPORT}"
	fi
	local state
	state="$(docker_cmd info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null || true)"
	if [[ "${state}" != "active" ]]; then
		fail "Docker context ${DOCKER_CONTEXT} is not attached to an active Swarm node"
	fi
}

wait_for_http() {
	local deadline=$((SECONDS + WAIT_SECONDS))
	while (( SECONDS < deadline )); do
		if harness_request GET "/status" >/dev/null 2>&1; then
			if [[ -n "${HARNESS_URL}" ]]; then
				log "harness HTTP is reachable at ${HARNESS_URL}"
			else
				log "harness HTTP is reachable through ${HARNESS_TRANSPORT} transport"
			fi
			return
		fi
		sleep 3
	done
	if [[ -n "${HARNESS_URL}" ]]; then
		fail "timed out waiting for harness HTTP at ${HARNESS_URL}"
	fi
	fail "timed out waiting for harness HTTP through ${HARNESS_TRANSPORT} transport"
}

harness_request() {
	local method="$1"
	local path="$2"
	if [[ -n "${HARNESS_URL}" ]]; then
		curl -fsS -X "${method}" "${HARNESS_URL}${path}"
		return
	fi
	local node
	node="$(running_task_node)"
	if [[ -z "${node}" ]]; then
		return 1
	fi
	case "${HARNESS_TRANSPORT}" in
		host)
			local addr
			addr="$(docker_cmd node inspect "${node}" --format '{{.Status.Addr}}')"
			if [[ -z "${addr}" ]]; then
				return 1
			fi
			curl -fsS --connect-timeout 2 --max-time 10 -X "${method}" "http://${addr}:${HARNESS_HOST_PORT}${path}"
			;;
		ssh)
			ssh -o BatchMode=yes -o ConnectTimeout=5 "${node}" \
				curl -fsS --connect-timeout 2 --max-time 10 -X "${method}" "http://127.0.0.1:${HARNESS_HOST_PORT}${path}"
			;;
		ssh-exec)
			local task
			task="$(running_task_name_for_node "${node}")"
			if [[ -z "${task}" ]]; then
				return 1
			fi
			ssh -o BatchMode=yes -o ConnectTimeout=5 "${node}" \
				"task=$(shell_quote "${task}") method=$(shell_quote "${method}") url=$(shell_quote "http://127.0.0.1:8080${path}") sh -s" <<'EOF'
container="$(docker ps --filter "label=com.docker.swarm.task.name=${task}" --format '{{.ID}}' | head -n1)"
if [ -z "${container}" ]; then
	container="$(docker ps --filter "name=${task}." --format '{{.ID}}' | head -n1)"
fi
if [ -z "${container}" ]; then
	exit 1
fi
docker exec "${container}" curl -fsS --connect-timeout 2 --max-time 10 -X "${method}" "${url}"
EOF
			;;
		*)
			fail "invalid HARNESS_TRANSPORT=${HARNESS_TRANSPORT}; use host, ssh, or ssh-exec"
			;;
	esac
}

shell_quote() {
	printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

running_task_node() {
	docker_cmd service ps "${SERVICE_NAME}" \
		--filter desired-state=running \
		--format '{{.Node}} {{.CurrentState}}' \
		| awk '$2 == "Running" { print $1; exit }'
}

running_task_name_for_node() {
	local node="$1"
	docker_cmd service ps "${SERVICE_NAME}" \
		--filter desired-state=running \
		--format '{{.Name}} {{.Node}} {{.CurrentState}}' \
		| awk -v node="${node}" '$2 == node && $3 == "Running" { print $1; exit }'
}

mark_phase() {
	local phase="$1"
	local deadline=$((SECONDS + WAIT_SECONDS))
	while (( SECONDS < deadline )); do
		if harness_request POST "/mark?phase=${phase}" >/dev/null; then
			log "marked phase ${phase}"
			return
		fi
		sleep 3
	done
	fail "timed out marking phase ${phase}"
}

build_image() {
	log "building ${IMAGE}"
	docker_cmd build -t "${IMAGE}" -f "${REPO_ROOT}/examples/swarm/app/Dockerfile" "${REPO_ROOT}"
}

deploy_stack() {
	log "deploying stack ${STACK_NAME}"
	(
		cd "${SCRIPT_DIR}"
		docker_cmd stack deploy -c docker-stack.yml "${STACK_NAME}"
	)
}

scale_service() {
	local replicas="$1"
	log "scaling ${SERVICE_NAME} to ${replicas}"
	docker_cmd service scale "${SERVICE_NAME}=${replicas}" >/dev/null
	wait_for_replicas "${replicas}"
}

force_update() {
	log "forcing rolling update for ${SERVICE_NAME}"
	docker_cmd service update --force "${SERVICE_NAME}" >/dev/null
	wait_for_replicas "${REPLICAS}"
	wait_for_update_completed
}

wait_for_update_completed() {
	local deadline=$((SECONDS + WAIT_SECONDS))
	while (( SECONDS < deadline )); do
		local state
		state="$(docker_cmd service inspect "${SERVICE_NAME}" --format '{{if .UpdateStatus}}{{.UpdateStatus.State}}{{else}}completed{{end}}' 2>/dev/null || true)"
		case "${state}" in
			"" | completed)
				log "${SERVICE_NAME} update completed"
				return
				;;
			paused | rollback_started | rollback_paused | rollback_completed)
				fail "${SERVICE_NAME} update ended in state ${state}"
				;;
		esac
		sleep 3
	done
	fail "timed out waiting for ${SERVICE_NAME} update completion"
}

wait_for_replicas() {
	local want="$1"
	local deadline=$((SECONDS + WAIT_SECONDS))
	while (( SECONDS < deadline )); do
		local running
		running="$(docker_cmd service ps "${SERVICE_NAME}" \
			--filter desired-state=running \
			--format '{{.CurrentState}}' \
			| grep -c '^Running ' || true)"
		if [[ "${running}" -ge "${want}" ]]; then
			log "${SERVICE_NAME} has ${running}/${want} running tasks"
			return
		fi
		sleep 3
	done
	fail "timed out waiting for ${want} running task(s)"
}

status_json() {
	harness_request GET "/status"
}

wait_for_status_json() {
	local deadline=$((SECONDS + WAIT_SECONDS))
	while (( SECONDS < deadline )); do
		local body
		body="$(status_json || true)"
		if [[ -n "${body}" ]] && grep -F '"NodeName"' <<<"${body}" >/dev/null; then
			printf '%s' "${body}"
			return
		fi
		sleep 3
	done
	fail "timed out waiting for harness status"
}

wait_for_status_json_for_node() {
	local node="$1"
	local deadline=$((SECONDS + WAIT_SECONDS))
	while (( SECONDS < deadline )); do
		local body
		body="$(wait_for_status_json || true)"
		if [[ "$(status_node_name "${body}")" == "${node}" ]]; then
			printf '%s' "${body}"
			return
		fi
		sleep 3
	done
	fail "timed out waiting for harness status from node ${node}"
}

status_number() {
	local body="$1"
	local key="$2"
	grep -o "\"${key}\":[0-9]*" <<<"${body}" | head -n1 | sed "s/.*://" || true
}

status_node_name() {
	local body="$1"
	grep -o '"NodeName":"[^"]*"' <<<"${body}" | head -n1 | sed 's/"NodeName":"//;s/"$//' || true
}

status_peer_problem_count() {
	local body="$1"
	(grep -o '"State":"identity_mismatch"\|"State":"unreachable"' <<<"${body}" || true) | wc -l | tr -d ' '
}

status_ready() {
	local body="$1"
	grep -o '"Ready":true\|"Ready":false' <<<"${body}" | head -n1 | sed 's/.*://' || true
}

status_verified_peers() {
	local body="$1"
	status_number "${body}" "VerifiedPeers"
}

status_min_ready_peers() {
	local body="$1"
	status_number "${body}" "MinReadyPeers"
}

check_peer_status() {
	local body="$1"
	local peer_problems
	peer_problems="$(status_peer_problem_count "${body}")"
	if [[ "${peer_problems}" != "0" ]]; then
		fail "found ${peer_problems} peer(s) in identity_mismatch or unreachable state"
	fi
}

check_expected_topology() {
	local body="$1"
	local expected_replicas="$2"
	local expected_peers=$((expected_replicas - 1))
	local verified min_ready
	verified="$(status_verified_peers "${body}")"
	min_ready="$(status_min_ready_peers "${body}")"
	if [[ -z "${verified}" ]]; then
		fail "cache status is missing VerifiedPeers"
	fi
	if (( verified < expected_peers )); then
		fail "verified peers ${verified} below expected ${expected_peers} for ${expected_replicas} replica(s)"
	fi
	if [[ -n "${min_ready}" ]] && (( expected_peers >= min_ready )) && [[ "$(status_ready "${body}")" != "true" ]]; then
		fail "cache status is not ready"
	fi
}

gossip_degraded_total() {
	local body="$1"
	status_number "${body}" "DegradedTotal"
}

check_gossip_degradation_delta() {
	local before="$1"
	local after="$2"
	local context="$3"
	case "${GOSSIP_DEGRADATION_MODE}" in
		off)
			return
			;;
		warn | fail)
			;;
		*)
			fail "invalid GOSSIP_DEGRADATION_MODE=${GOSSIP_DEGRADATION_MODE}; use off, warn, or fail"
			;;
	esac
	if [[ -z "${before}" || -z "${after}" ]]; then
		fail "status response missing Gossip.DegradedTotal"
	fi
	if (( after <= before )); then
		return
	fi
	if [[ "${context}" == "intentional-churn" ]]; then
		log "INFO: memberlist gossip degraded events increased from ${before} to ${after} during intentional churn"
		return
	fi
	if [[ "${GOSSIP_DEGRADATION_MODE}" == "fail" ]]; then
		fail "memberlist gossip degraded events increased from ${before} to ${after}"
	fi
	log "WARN: memberlist gossip degraded events increased from ${before} to ${after}"
}

prove_convergence() {
	local phase="$1"
	local gossip_context="${2:-steady-state}"
	local expected_replicas="${3:-${REPLICAS}}"
	local before_status before_node before_gossip after_status after_gossip expected_value
	before_status="$(wait_for_status_json)"
	before_node="$(status_node_name "${before_status}")"
	if [[ -z "${before_node}" ]]; then
		fail "status response missing NodeName"
	fi
	before_gossip="$(gossip_degraded_total "${before_status}")"
	mark_phase "${phase}"
	expected_value="phase-${phase}-from-"
	wait_for_cache_value_prefix "swarm-key" "${expected_value}"
	after_status="$(wait_for_status_json_for_node "${before_node}")"
	after_gossip="$(gossip_degraded_total "${after_status}")"
	check_expected_topology "${after_status}" "${expected_replicas}"
	check_peer_status "${after_status}"
	check_gossip_degradation_delta "${before_gossip}" "${after_gossip}" "${gossip_context}"
}

cache_set() {
	local key="$1"
	local value="$2"
	local deadline=$((SECONDS + WAIT_SECONDS))
	while (( SECONDS < deadline )); do
		if harness_request POST "/set?key=${key}&value=${value}&ttl_ms=600000" >/dev/null; then
			return
		fi
		sleep 3
	done
	fail "timed out setting cache value ${key}"
}

cache_del() {
	local key="$1"
	local deadline=$((SECONDS + WAIT_SECONDS))
	while (( SECONDS < deadline )); do
		if harness_request POST "/del?key=${key}" >/dev/null; then
			return
		fi
		sleep 3
	done
	fail "timed out deleting cache value ${key}"
}

cache_get() {
	local key="$1"
	harness_request GET "/get?key=${key}"
}

wait_for_cache_value() {
	local key="$1"
	local value="$2"
	local deadline=$((SECONDS + WAIT_SECONDS))
	while (( SECONDS < deadline )); do
		local body
		body="$(cache_get "${key}" || true)"
		if grep -F '"found":true' <<<"${body}" >/dev/null && grep -F "\"value\":\"${value}\"" <<<"${body}" >/dev/null; then
			log "observed cache value ${key}=${value}"
			return
		fi
		sleep 3
	done
	fail "timed out waiting for cache value ${key}=${value}"
}

wait_for_cache_value_prefix() {
	local key="$1"
	local value_prefix="$2"
	local deadline=$((SECONDS + WAIT_SECONDS))
	while (( SECONDS < deadline )); do
		local body
		body="$(cache_get "${key}" || true)"
		if grep -F '"found":true' <<<"${body}" >/dev/null && grep -F "\"value\":\"${value_prefix}" <<<"${body}" >/dev/null; then
			log "observed cache value ${key} prefix ${value_prefix}"
			return
		fi
		sleep 3
	done
	fail "timed out waiting for cache value ${key} prefix ${value_prefix}"
}

wait_for_cache_miss() {
	local key="$1"
	local deadline=$((SECONDS + WAIT_SECONDS))
	while (( SECONDS < deadline )); do
		local body
		body="$(cache_get "${key}" || true)"
		if grep -F '"found":false' <<<"${body}" >/dev/null; then
			log "observed cache miss for ${key}"
			return
		fi
		sleep 3
	done
	fail "timed out waiting for cache miss ${key}"
}

prove_active_cache_api() {
	local key="chaos-key"
	local value="chaos-value-$(date +%s)"
	cache_set "${key}" "${value}"
	wait_for_cache_value "${key}" "${value}"
	cache_del "${key}"
	wait_for_cache_miss "${key}"
}

prove_steady_state() {
	local expected_replicas="${1:-${REPLICAS}}"
	log "waiting ${STEADY_SECONDS}s before steady-state gossip check"
	sleep "${STEADY_SECONDS}"
	prove_convergence "steady-$(date +%s)" "steady-state" "${expected_replicas}"
	prove_active_cache_api
}

cleanup_stack() {
	if [[ "${CLEANUP:-0}" == "1" ]]; then
		log "removing stack ${STACK_NAME}"
		if ! docker_cmd stack rm "${STACK_NAME}" >/dev/null; then
			log "WARN: stack removal reported an error; Docker may still be converging cleanup"
		fi
	fi
}

main() {
	trap cleanup_stack EXIT
	require_swarm
	build_image
	deploy_stack
	wait_for_replicas "${REPLICAS}"
	wait_for_http
	prove_convergence "deploy-$(date +%s)" "intentional-churn" "${REPLICAS}"
	prove_active_cache_api

	scale_service 1
	wait_for_http
	prove_convergence "scale-down-$(date +%s)" "intentional-churn" 1
	prove_active_cache_api

	scale_service "${REPLICAS}"
	wait_for_http
	prove_convergence "scale-up-$(date +%s)" "intentional-churn" "${REPLICAS}"
	prove_active_cache_api

	force_update
	wait_for_http
	prove_convergence "force-update-$(date +%s)" "intentional-churn" "${REPLICAS}"
	prove_active_cache_api
	prove_steady_state "${REPLICAS}"

	log "PASS: example stack converged across deploy, scale churn, and forced update"
}

main "$@"
