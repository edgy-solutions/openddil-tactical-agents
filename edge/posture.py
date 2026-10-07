"""
Posture state machine (ADR-0044 amendment, "posture, a third column").

Rules, mirroring detection/algorithms.py's framework-agnostic contract:
1. Pure function: (prev_state, launcher_raised, speed, now, thresholds) ->
   new_state. No I/O, no Faust types, no proto types, no logging.
2. The caller (edge/faust_edge.py) is responsible for pulling launcher_raised,
   speed and now out of the proto event and for converting the string state
   names here into whatever wire representation (proto enum, DB text) it
   needs. This module never imports openddil.telemetry.

WHAT THIS DECIDES, AND WHAT IT DOES NOT:
  posture is a DECIDED value (emplaced / march_ordered / moving / emplacing /
  unspecified), computed from two independent inputs that may each be absent
  on any given record:
    - launcher_raised: True / False / None. None means "this platform's
      domain has no launcher bit at all" (e.g. air), not "unknown this tick".
    - speed: a float m/s, or None meaning "no velocity reading this tick"
      (hold whatever motion state we already had; never guess a value).
  motion (moving / stationary / "") is an internal input to posture, not
  itself exposed as a posture value. "" means "never observed a speed
  reading for this asset" (the cold-start case) and is distinct from
  "stationary" (observed speed <= threshold).

  This module never changes operational_status (readiness/FMC-NMC-PMC);
  posture is decided at the owning tier and carried alongside it, not folded
  into it (see the ADR-0044 amendment for why a column, not a status value).

TRANSITION TABLE (first matching row wins; evaluated in this order):
  1. posture != "moving" AND motion == "moving" held >= move_hold_s
       -> moving                                   (launcher_raised ignored)
  2. posture == "moving" AND motion == "stationary" held >= stop_hold_s
       -> launcher_raised is None:  unspecified     (no launcher signal to
                                                      disambiguate — never
                                                      guess emplaced/emplacing)
       -> launcher_raised is True:  emplaced
       -> launcher_raised is False: emplacing
  3. posture in {emplacing, unspecified, march_ordered} AND motion ==
     "stationary" AND launcher_raised is True
       -> emplaced                                 (immediate, no hold)
  4. posture == "emplaced" AND launcher_raised is False
       -> march_ordered                             (immediate, no hold)
  (cold start, posture defaults to "unspecified"; a first reading of
  stationary + stowed matches none of the above and correctly stays
  unspecified rather than guessing — this is COLD_START's own result, not a
  separate row.)
"""
from __future__ import annotations

from typing import NamedTuple, Optional


class PostureState(NamedTuple):
    posture: str          # "unspecified" | "emplaced" | "march_ordered" | "moving" | "emplacing"
    posture_since: Optional[float]
    motion: str           # "moving" | "stationary" | "" (never observed)
    motion_since: Optional[float]


class Thresholds(NamedTuple):
    move_speed_mps: float
    move_hold_s: float
    stop_hold_s: float


UNSPECIFIED = "unspecified"
EMPLACED = "emplaced"
MARCH_ORDERED = "march_ordered"
MOVING = "moving"
EMPLACING = "emplacing"

# The state an asset is in before any record has ever been seen for it. A
# record read at cold start that happens to show stationary+stowed matches
# none of the transition rows below and so correctly stays UNSPECIFIED
# (never guessed) — see the ADR-0044 amendment's transition table.
COLD_START = PostureState(posture=UNSPECIFIED, posture_since=None, motion="", motion_since=None)


def step(
    prev: PostureState,
    launcher_raised: Optional[bool],
    speed: Optional[float],
    now: float,
    th: Thresholds,
) -> PostureState:
    """Advance the posture state machine by one record. Pure; no side effects."""
    # --- motion update -------------------------------------------------
    # speed is None: "no motion update" -- hold whatever motion state (and
    # its since-timestamp) we already had, verbatim.
    if speed is None:
        motion, motion_since = prev.motion, prev.motion_since
    else:
        observed = MOVING if speed > th.move_speed_mps else "stationary"
        if observed == prev.motion:
            # Same observation as before: keep the existing since-timestamp
            # (or start it now, if this is the very first reading at this
            # value and prev.motion_since was never set).
            motion = prev.motion
            motion_since = prev.motion_since if prev.motion_since is not None else now
        else:
            # Motion value changed (including the cold-start "" -> first
            # real reading transition) -- the hold clock restarts now.
            motion, motion_since = observed, now

    held = (now - motion_since) if motion_since is not None else 0.0

    # --- posture update -------------------------------------------------
    posture, posture_since = prev.posture, prev.posture_since

    def _to(new_posture: str) -> None:
        nonlocal posture, posture_since
        if new_posture != posture:
            posture, posture_since = new_posture, now

    if posture != MOVING and motion == MOVING and held >= th.move_hold_s:
        _to(MOVING)
    elif posture == MOVING and motion == "stationary" and held >= th.stop_hold_s:
        if launcher_raised is None:
            _to(UNSPECIFIED)
        elif launcher_raised:
            _to(EMPLACED)
        else:
            _to(EMPLACING)
    elif (
        posture in (EMPLACING, UNSPECIFIED, MARCH_ORDERED)
        and motion == "stationary"
        and launcher_raised is True
    ):
        _to(EMPLACED)
    elif posture == EMPLACED and launcher_raised is False:
        _to(MARCH_ORDERED)

    return PostureState(posture=posture, posture_since=posture_since, motion=motion, motion_since=motion_since)
