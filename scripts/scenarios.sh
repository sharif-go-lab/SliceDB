#!/usr/bin/env bash
# Runs the test scenarios of the assignment against the docker compose deployment.
#
#   scripts/scenarios.sh <1-7|all>
#
# 1 single node   2 multi node / RPS   3 node failure   4 rolling restart
# 5 add node under load   6 replica count up/down   7 etcd + controller failures (exercise 4)
set -euo pipefail
cd "$(dirname "$0")/.."

DC="docker compose"
CONTROLLERS=(8080 8081 8082)
BENCH_TIME=${BENCH_TIME:-20s}

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
client() { $DC --profile tools run --rm -T client "$@"; }

# api PATH [JSON]: call the admin API on whichever controller answers (they forward to the leader)
api() {
	for port in "${CONTROLLERS[@]}"; do
		if [ $# -gt 1 ]; then
			curl -fsS -X POST -H 'Content-Type: application/json' -d "$2" "http://localhost:$port$1" 2>/dev/null && echo && return 0
		else
			curl -fsS "http://localhost:$port$1" 2>/dev/null && echo && return 0
		fi
	done
	echo "no controller reachable" >&2
	return 1
}

wait_stable() {
	local reason=""
	for _ in $(seq 1 120); do
		for port in "${CONTROLLERS[@]}"; do
			if curl -fsS "http://localhost:$port/api/stable" >/dev/null 2>&1; then
				echo "cluster is stable"
				return 0
			fi
		done
		reason=$(curl -sS "http://localhost:${CONTROLLERS[0]}/api/stable" 2>/dev/null || true)
		sleep 1
	done
	echo "cluster did not converge: $reason" >&2
	return 1
}

leader_controller() {
	api /api/state | sed -n 's/.*"leader":"\([^":]*\):.*/\1/p'
}

fresh() {
	log "fresh deployment: $*"
	$DC --profile extra --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
	$DC up -d --build "$@"
	wait_stable
}

scenario1() {
	log "1. one database node + controller + load balancer"
	fresh etcd1 etcd2 etcd3 controller1 node1 lb1
	client set user:1 alice
	client get user:1
	client del user:1
	client get user:1 || echo "(not found after delete, as expected)"
	client fill -n 2000 && client verify -n 2000
	client bench -duration "$BENCH_TIME" -concurrency 32
}

scenario2() {
	log "2. several nodes and partitions: does RPS grow with nodes / replicas?"
	fresh
	log "3 nodes, replication factor 2"
	client bench -duration "$BENCH_TIME" -concurrency 64
	log "adding node4 and node5"
	$DC --profile extra up -d node4 node5
	wait_stable
	client bench -duration "$BENCH_TIME" -concurrency 64
	log "replication factor 3 (more replicas to read from)"
	api /api/config '{"replication_factor":3}'
	wait_stable
	client bench -duration "$BENCH_TIME" -concurrency 64 -reads 0.95
}

scenario3() {
	log "3. stopping a node: its partitions fail over"
	fresh
	client fill -n 5000
	client bench -duration 25s -concurrency 16 -reads 0.5 &
	sleep 8
	log "stopping node2"
	$DC stop node2
	wait
	wait_stable
	client verify -n 5000
	api /api/state | grep -o '"message":"[^"]*node2[^"]*"' | tail -5 || true
}

scenario4() {
	log "4. restarting the nodes one after the other"
	fresh
	client fill -n 5000
	client bench -duration 60s -concurrency 8 -reads 0.7 &
	for n in node1 node2 node3; do
		log "restarting $n"
		$DC restart "$n"
		wait_stable
		client verify -n 5000
	done
	wait
}

scenario5() {
	log "5. adding a node while the cluster is under load"
	fresh
	client bench -duration 45s -concurrency 32 -reads 0.7 &
	sleep 12
	log "starting node4"
	$DC --profile extra up -d node4
	wait
	wait_stable
	api /api/state | grep -o '"message":"rebalance[^"]*"' | head -10 || true
}

scenario6() {
	log "6. more and fewer replicas"
	fresh
	client fill -n 5000
	log "replication factor 2 -> 3"
	api /api/config '{"replication_factor":3}'
	wait_stable
	client verify -n 5000
	log "two of three nodes fail at once; the third still has every key"
	$DC kill node1 node2
	sleep 8
	client verify -n 5000
	$DC start node1 node2
	wait_stable
	log "replication factor 3 -> 1"
	api /api/config '{"replication_factor":1}'
	wait_stable
	client verify -n 5000
	log "resharding 8 -> 16 partitions under load"
	api /api/config '{"replication_factor":2}'
	wait_stable
	client bench -duration 20s -concurrency 16 -reads 0.5 &
	sleep 3
	api /api/config '{"partition_count":16}'
	wait
	wait_stable
	client verify -n 5000
}

scenario7() {
	log "7. no single point of failure (exercise 4): etcd member + leader controller die"
	fresh
	client fill -n 5000
	client bench -duration 40s -concurrency 16 -reads 0.5 &
	sleep 5
	leader=$(leader_controller)
	log "killing the leader controller ($leader) and etcd1"
	$DC kill "$leader" etcd1
	sleep 8
	log "new leader: $(leader_controller)"
	log "a node fails while the original controller is gone: the new leader handles it"
	$DC kill node3
	wait
	wait_stable
	client verify -n 5000
	$DC start "$leader" etcd1 node3
	wait_stable
}

case "${1:-}" in
	1 | 2 | 3 | 4 | 5 | 6 | 7) "scenario$1" ;;
	all) for i in 1 2 3 4 5 6 7; do "scenario$i"; done ;;
	*) sed -n '2,8p' "$0"; exit 2 ;;
esac
