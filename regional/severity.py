"""Pure-function severity-bucket assignment for the region-fleet-summary
aggregator. Kept separate from Faust glue so the rules are unit-testable
and can be reviewed against the §B recipe in isolation.

ADR-0023 Phase 6b §B. Bucket assignment is the WORST of an asset's
logistics severity and its cm-state derived severity. Sources:

  asset-logistics-status   — AssetLogisticsStatusUpdate.status.overall_severity
                              (openddil.logistics.v1.LogisticsSeverity)
  asset-cm-state           — JSON envelope, top-level overall_status
                              (openddil.configuration.v1.am.ConfigurationStatus)
                              + lifecycle (am.Lifecycle)

The integer values of each enum are stable and well-known; we keep the
mapping in this file rather than importing the proto modules to keep
the severity rule independent of proto regeneration timing (the rule
predates and outlasts any individual proto change).
"""
from __future__ import annotations

from typing import Literal

Bucket = Literal["nominal", "degraded", "critical", "non_operational"]

# LogisticsSeverity enum values (from logistics_status.proto)
_LOG_OK = 1
_LOG_DEGRADED = 2
_LOG_CRITICAL = 3
_LOG_NON_OPERATIONAL = 4

# ConfigurationStatus enum values (from as_maintained.proto)
_CM_IN_COMPLIANCE = 1
_CM_MINOR = 2
_CM_MAJOR = 3
_CM_NMC = 4

# Lifecycle enum values (from as_maintained.proto)
_LC_REGISTERED = 1
_LC_ACTIVE = 2
_LC_STALE = 3
_LC_DECOMMISSIONED = 4

# Rank order — higher index = worse bucket. Used to combine logistics and
# cm-state buckets via max-by-rank.
_BUCKET_RANK = {
    "nominal": 0,
    "degraded": 1,
    "critical": 2,
    "non_operational": 3,
}


def bucket_from_logistics_severity(severity: int) -> Bucket:
    if severity == _LOG_NON_OPERATIONAL:
        return "non_operational"
    if severity == _LOG_CRITICAL:
        return "critical"
    if severity == _LOG_DEGRADED:
        return "degraded"
    return "nominal"


def bucket_from_cm_state(overall_status: int, lifecycle: int) -> Bucket:
    # Lifecycle dominates when an asset is decommissioned — that's a
    # NON_OPERATIONAL state regardless of overall_status.
    if lifecycle == _LC_DECOMMISSIONED:
        return "non_operational"
    if overall_status == _CM_NMC:
        return "non_operational"
    if overall_status == _CM_MAJOR:
        return "critical"
    if overall_status == _CM_MINOR or lifecycle == _LC_STALE:
        return "degraded"
    return "nominal"


def worse_bucket(a: Bucket, b: Bucket) -> Bucket:
    """Return whichever bucket is worse (higher rank). Used to combine
    per-asset buckets from logistics and cm-state into the asset's final
    bucket."""
    return a if _BUCKET_RANK[a] >= _BUCKET_RANK[b] else b
