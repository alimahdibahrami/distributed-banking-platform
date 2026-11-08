"""Opt-in, fail-closed alignment of replica identity and durable ownership.

A routing epoch agreement between the Gateway and Coordinator is not enough:
replicas also need to prove that their database has the expected post-cutover
identity/fence before being selected. This module never performs a cutover.
"""
from __future__ import annotations

import os

from bank_platform.migration.epoch import enabled as epoch_propagation_enabled
from bank_platform.migration.routing_guard import strict_routing


def cohort_required() -> bool:
    return os.getenv('BANK_REQUIRE_SHARD_COHORT', '0') == '1'


def configured_epoch() -> int:
    raw = os.getenv('BANK_ROUTING_EPOCH', '')
    if not raw.isascii() or not raw.isdecimal() or raw.startswith('0'):
        raise ValueError('BANK_ROUTING_EPOCH must be an explicit positive integer')
    value = int(raw)
    if value not in (1, 2):
        raise ValueError('Only assignment routing epochs 1 and 2 are supported')
    return value


def assert_cohort_configuration(*, bank: bool = False) -> None:
    if not cohort_required():
        return
    if not strict_routing() or not epoch_propagation_enabled():
        raise ValueError('BANK_REQUIRE_SHARD_COHORT requires routing epoch gate and propagation')
    if bank:
        configured_epoch()


def replica_proves_epoch(payload: object, shard: str, replica: str, epoch: int) -> bool:
    """Transport-facing proof; never trust /health as ownership evidence."""
    return (isinstance(payload, dict)
            and payload.get('status') == 'ready'
            and payload.get('shard') == shard
            and payload.get('replica') == replica
            and payload.get('storage_ready') is True
            and payload.get('banking_ready') is True
            and payload.get('ownership_verified') is True
            and type(payload.get('routing_epoch')) is int
            and payload['routing_epoch'] == epoch)
