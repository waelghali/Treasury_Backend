# app/services/desk_session_service.py
"""
Desk Session Manager Service
Manages real-time multi-dealer concurrency on bank counterparty quotation desks.
Provides active trader exclusive quoting lock with soft takeover and heartbeat TTL.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
import threading
import logging

logger = logging.getLogger(__name__)

HEARTBEAT_TIMEOUT_SECONDS = 45  # Inactivity threshold to release active lock


class DeskState:
    def __init__(self, assignment_id: str):
        self.assignment_id = assignment_id
        self.active_trader_email: Optional[str] = None
        self.active_trader_name: Optional[str] = None
        self.active_session_token: Optional[str] = None
        self.active_since: Optional[datetime] = None
        self.last_heartbeat: Optional[datetime] = None
        
        # Spectator colleagues currently viewing this desk: session_key -> dict
        self.spectators: Dict[str, Dict[str, Any]] = {}

        # Latest quote mirrored state
        self.last_quote_price: Optional[float] = None
        self.last_quote_legs: Optional[Dict[str, Any]] = None
        self.last_passed_legs: Optional[List[str]] = None
        self.last_quote_by: Optional[str] = None
        self.last_quote_at: Optional[datetime] = None

    def clean_inactive_spectators(self, now: datetime):
        """Purges spectators that haven't sent a heartbeat within the timeout window or belong to the active dealer."""
        cutoff = now - timedelta(seconds=HEARTBEAT_TIMEOUT_SECONDS)
        to_remove = []
        for key, spec in self.spectators.items():
            last_seen = spec.get("last_seen")
            spec_email = (spec.get("email") or "").strip().lower()
            if last_seen and last_seen < cutoff:
                to_remove.append(key)
            elif self.active_trader_email and spec_email == self.active_trader_email:
                to_remove.append(key)
        for key in to_remove:
            self.spectators.pop(key, None)

    def check_and_release_expired_lock(self, now: datetime):
        """Releases the active trader lock if their heartbeat has expired."""
        if self.active_trader_email and self.last_heartbeat:
            if (now - self.last_heartbeat).total_seconds() > HEARTBEAT_TIMEOUT_SECONDS:
                logger.info(
                    f"Desk {self.assignment_id}: Active lock for {self.active_trader_email} expired after {HEARTBEAT_TIMEOUT_SECONDS}s inactivity."
                )
                self.active_trader_email = None
                self.active_trader_name = None
                self.active_session_token = None
                self.active_since = None
                self.last_heartbeat = None


class DeskSessionService:
    def __init__(self):
        self._desks: Dict[str, DeskState] = {}
        self._lock = threading.Lock()

    def _get_or_create(self, assignment_id: str) -> DeskState:
        if assignment_id not in self._desks:
            self._desks[assignment_id] = DeskState(assignment_id)
        return self._desks[assignment_id]

    def get_or_claim_desk(
        self,
        assignment_id: str,
        email: str,
        name: Optional[str] = None,
        role: str = "EXECUTION",
        session_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Registers a dealer on the desk.
        If no active dealer exists or the prior lock expired, claims active control.
        If the caller is already the active dealer (same email), re-establishes active control
        and updates the active session token (e.g. across page refreshes).
        Otherwise, registers the dealer as a spectator.
        """
        with self._lock:
            desk = self._get_or_create(assignment_id)
            now = datetime.now(timezone.utc)
            desk.clean_inactive_spectators(now)
            desk.check_and_release_expired_lock(now)

            clean_email = email.strip().lower()
            display_name = name or clean_email.split("@")[0]
            clean_role = (role or "EXECUTION").strip().upper()
            session_key = session_token or clean_email

            # CRITICAL RULE: Non-execution roles (APPROVER, VIEW_ONLY) are strictly observers!
            # They must NEVER claim or hold active execution control of the desk.
            if clean_role != "EXECUTION":
                desk.spectators[session_key] = {
                    "email": clean_email,
                    "name": display_name,
                    "role": clean_role,
                    "session_token": session_token,
                    "last_seen": now
                }
                # If this non-execution user was previously recorded as active trader, release it!
                if desk.active_trader_email == clean_email:
                    desk.active_trader_email = None
                    desk.active_trader_name = None
                    desk.active_session_token = None
                    desk.active_since = None
                    desk.last_heartbeat = None
                return self._build_status(desk, clean_email, is_active=False, session_token=session_token)

            # Case 1: Caller IS ALREADY the active dealer (same email)
            if desk.active_trader_email == clean_email:
                # If this is the active tab session, or the desk has no active session token recorded
                if not desk.active_session_token or (session_token and desk.active_session_token == session_token):
                    desk.last_heartbeat = now
                    desk.active_session_token = session_token
                    if display_name:
                        desk.active_trader_name = display_name
                    # Active tab cannot be in spectators list
                    to_remove = [k for k, s in desk.spectators.items() if (s.get("email") or "").strip().lower() == clean_email]
                    for k in to_remove:
                        desk.spectators.pop(k, None)
                    return self._build_status(desk, clean_email, is_active=True, session_token=session_token)
                else:
                    # Same dealer, but from a SECONDARY tab or window!
                    # Maintain active quoting on the primary tab; do not overwrite active_session_token.
                    # Track secondary tab in spectators, but note that _build_status filters it from colleague count.
                    desk.spectators[session_key] = {
                        "email": clean_email,
                        "name": display_name,
                        "role": clean_role,
                        "session_token": session_token,
                        "last_seen": now
                    }
                    return self._build_status(desk, clean_email, is_active=False, session_token=session_token)

            # Case 2: Desk has no active trader -> Claim it
            elif desk.active_trader_email is None:
                desk.active_trader_email = clean_email
                desk.active_trader_name = display_name
                desk.active_session_token = session_token
                desk.active_since = now
                desk.last_heartbeat = now
                to_remove = [k for k, s in desk.spectators.items() if (s.get("email") or "").strip().lower() == clean_email]
                for k in to_remove:
                    desk.spectators.pop(k, None)
                logger.info(f"Desk {assignment_id}: Active lock claimed by {clean_email} (session {session_token})")
                return self._build_status(desk, clean_email, is_active=True, session_token=session_token)

            # Case 3: A different trader (different email) is currently active -> Caller is a spectator
            else:
                desk.spectators[session_key] = {
                    "email": clean_email,
                    "name": display_name,
                    "role": clean_role,
                    "session_token": session_token,
                    "last_seen": now
                }
                return self._build_status(desk, clean_email, is_active=False, session_token=session_token)

    def heartbeat(
        self,
        assignment_id: str,
        email: str,
        name: Optional[str] = None,
        role: str = "EXECUTION",
        session_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Refreshes heartbeat for dealer and returns latest desk state."""
        return self.get_or_claim_desk(assignment_id, email, name, role, session_token=session_token)

    def takeover_desk(
        self,
        assignment_id: str,
        email: str,
        name: Optional[str] = None,
        role: str = "EXECUTION",
        session_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Transfers active execution control to the requesting dealer immediately.
        Demotes any previous active dealer to spectator.
        """
        with self._lock:
            desk = self._get_or_create(assignment_id)
            now = datetime.now(timezone.utc)
            clean_email = email.strip().lower()
            display_name = name or clean_email.split("@")[0]
            clean_role = (role or "EXECUTION").strip().upper()
            session_key = session_token or clean_email

            # Non-execution users cannot take over desk control
            if clean_role != "EXECUTION":
                return self._build_status(desk, clean_email, is_active=False, session_token=session_token)

            prev_trader = desk.active_trader_email
            prev_session = desk.active_session_token
            # Demote previous trader ONLY if it's genuinely a different colleague
            if prev_trader and prev_trader != clean_email:
                demoted_key = prev_session or prev_trader
                desk.spectators[demoted_key] = {
                    "email": prev_trader,
                    "name": desk.active_trader_name or prev_trader.split("@")[0],
                    "role": "EXECUTION",
                    "session_token": prev_session,
                    "last_seen": now
                }
                logger.info(f"Desk {assignment_id}: {clean_email} took over desk from {prev_trader}")

            desk.active_trader_email = clean_email
            desk.active_trader_name = display_name
            desk.active_session_token = session_token
            desk.active_since = now
            desk.last_heartbeat = now
            # Purge any spectator entries for this user
            to_remove = [k for k, s in desk.spectators.items() if (s.get("email") or "").strip().lower() == clean_email]
            for k in to_remove:
                desk.spectators.pop(k, None)

            return self._build_status(
                desk,
                clean_email,
                is_active=True,
                superseded_trader=prev_trader if prev_trader != clean_email else None,
                session_token=session_token
            )

    def reset_desk(self, assignment_id: str):
        """Clears all session locks and state for an assignment desk."""
        with self._lock:
            self._desks.pop(assignment_id, None)

    def release_desk(self, assignment_id: str, email: str, session_token: Optional[str] = None) -> bool:
        """Voluntary release of the active lock by the active trader."""
        with self._lock:
            desk = self._desks.get(assignment_id)
            if not desk:
                return False
            clean_email = email.strip().lower()
            session_key = session_token or clean_email
            if desk.active_session_token and session_token and desk.active_session_token == session_token:
                desk.active_trader_email = None
                desk.active_trader_name = None
                desk.active_session_token = None
                desk.active_since = None
                desk.last_heartbeat = None
                return True
            elif desk.active_trader_email == clean_email:
                desk.active_trader_email = None
                desk.active_trader_name = None
                desk.active_session_token = None
                desk.active_since = None
                desk.last_heartbeat = None
                return True
            desk.spectators.pop(session_key, None)
            return True

    def can_submit_quote(self, assignment_id: str, email: str, session_token: Optional[str] = None) -> (bool, Optional[str]):
        """
        Validates whether the specified email/session is permitted to submit quotes.
        Returns (True, None) if permitted, or (False, reason) if blocked.
        """
        with self._lock:
            desk = self._desks.get(assignment_id)
            if not desk:
                return True, None  # Desk uninitialized -> permit

            now = datetime.now(timezone.utc)
            desk.check_and_release_expired_lock(now)

            clean_email = email.strip().lower()
            if desk.active_trader_email is None:
                return True, None

            # Active dealer (matching email) check
            if desk.active_trader_email == clean_email:
                if desk.active_session_token and session_token and desk.active_session_token != session_token:
                    return False, "Quoting session is currently active in another tab or window. Click 'Resume Quoting in This Tab' to submit quotes here."
                return True, None

            return False, f"Desk is actively controlled by {desk.active_trader_name or desk.active_trader_email}. Click 'Take Over Desk' to submit quotes."

    def record_quote_submission(
        self, 
        assignment_id: str, 
        email: str, 
        price: Optional[float] = None,
        legs_quotes: Optional[Dict[str, Any]] = None,
        passed_legs: Optional[List[str]] = None
    ):
        """Mirrors latest submitted quote and passed legs to all desk observers."""
        with self._lock:
            desk = self._get_or_create(assignment_id)
            desk.last_quote_price = price
            desk.last_quote_legs = legs_quotes
            if passed_legs is not None:
                desk.last_passed_legs = [str(p) for p in passed_legs]
            desk.last_quote_by = email.strip().lower()
            desk.last_quote_at = datetime.now(timezone.utc)

    def _build_status(
        self,
        desk: DeskState,
        caller_email: str,
        is_active: bool,
        superseded_trader: Optional[str] = None,
        session_token: Optional[str] = None
    ) -> Dict[str, Any]:
        caller_clean = (caller_email or "").strip().lower()
        is_same_dealer = bool(caller_clean and desk.active_trader_email and desk.active_trader_email == caller_clean)

        is_current_session = False
        if is_same_dealer:
            if desk.active_session_token and session_token:
                is_current_session = (desk.active_session_token == session_token)
            else:
                is_current_session = True

        is_another_tab_active = bool(is_same_dealer and not is_current_session)
        is_active_trader = bool(is_active and is_current_session)

        # Deduplicate and filter colleague spectators by email:
        # 1. Do NOT include caller_email (a user cannot be their own colleague)
        # 2. Do NOT include desk.active_trader_email (active dealer cannot be a spectator colleague)
        # 3. Deduplicate multiple tabs/sessions from the same colleague into 1 single contact
        other_spectators = []
        seen_emails = set()
        for k, s in desk.spectators.items():
            spec_email = (s.get("email") or "").strip().lower()
            if not spec_email:
                continue
            if caller_clean and spec_email == caller_clean:
                continue
            if desk.active_trader_email and spec_email == desk.active_trader_email:
                continue
            if spec_email in seen_emails:
                continue
            seen_emails.add(spec_email)
            other_spectators.append({
                "email": spec_email,
                "name": s.get("name") or spec_email.split("@")[0],
                "role": s.get("role", "EXECUTION")
            })

        active_info = None
        if desk.active_trader_email:
            active_info = {
                "email": desk.active_trader_email,
                "name": desk.active_trader_name or desk.active_trader_email.split("@")[0],
                "active_since": desk.active_since.isoformat() if desk.active_since else None,
                "is_you": is_same_dealer,
                "is_current_tab": is_current_session
            }

        execution_colleagues = [s for s in other_spectators if s.get("role") == "EXECUTION"]

        return {
            "assignment_id": desk.assignment_id,
            "is_active_trader": is_active_trader,
            "is_another_tab_active": is_another_tab_active,
            "active_trader_email": desk.active_trader_email,
            "active_trader_name": desk.active_trader_name,
            "active_controller": active_info,
            "spectators_count": len(other_spectators),
            "colleagues_online": other_spectators,
            "execution_colleagues_count": len(execution_colleagues),
            "superseded_trader": superseded_trader,
            "mirrored_quote": {
                "price": desk.last_quote_price,
                "legs_quotes": desk.last_quote_legs,
                "passed_legs": [str(p) for p in (desk.last_passed_legs or [])],
                "by": desk.last_quote_by,
                "at": desk.last_quote_at.isoformat() if desk.last_quote_at else None
            }
        }


desk_session_service = DeskSessionService()
