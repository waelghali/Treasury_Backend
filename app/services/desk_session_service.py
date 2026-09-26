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
        self.last_quote_by: Optional[str] = None
        self.last_quote_at: Optional[datetime] = None

    def clean_inactive_spectators(self, now: datetime):
        """Purges spectators that haven't sent a heartbeat within the timeout window."""
        cutoff = now - timedelta(seconds=HEARTBEAT_TIMEOUT_SECONDS)
        to_remove = []
        for key, spec in self.spectators.items():
            last_seen = spec.get("last_seen")
            if last_seen and last_seen < cutoff:
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
        Otherwise, registers the dealer as a spectator.
        Supports session_token tracking for multi-tab testing.
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

            # Determine whether caller is the active session
            is_same_session = False
            if desk.active_session_token and session_token:
                is_same_session = (desk.active_session_token == session_token)
            elif desk.active_trader_email:
                is_same_session = (desk.active_trader_email == clean_email)

            # Case 1: Caller is already active trader
            if desk.active_trader_email and is_same_session:
                desk.last_heartbeat = now
                is_active = True
                if session_token and not desk.active_session_token:
                    desk.active_session_token = session_token
            # Case 2: Desk has no active trader -> Claim it
            elif desk.active_trader_email is None:
                desk.active_trader_email = clean_email
                desk.active_trader_name = display_name
                desk.active_session_token = session_token
                desk.active_since = now
                desk.last_heartbeat = now
                desk.spectators.pop(session_key, None)
                is_active = True
                logger.info(f"Desk {assignment_id}: Active lock claimed by {clean_email} (session {session_token})")
            # Case 3: Someone else is active -> Caller is a spectator
            else:
                is_active = False
                desk.spectators[session_key] = {
                    "email": clean_email,
                    "name": display_name,
                    "role": clean_role,
                    "session_token": session_token,
                    "last_seen": now
                }

            return self._build_status(desk, clean_email, is_active, session_token=session_token)

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
            if (prev_trader and prev_trader != clean_email) or (prev_session and prev_session != session_token):
                # Move previous active trader to spectators
                demoted_key = prev_session or prev_trader
                desk.spectators[demoted_key] = {
                    "email": prev_trader,
                    "name": desk.active_trader_name or (prev_trader.split("@")[0] if prev_trader else "Trader"),
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
            desk.spectators.pop(session_key, None)

            return self._build_status(desk, clean_email, is_active=True, superseded_trader=prev_trader, session_token=session_token)

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

            if session_token and desk.active_session_token:
                if desk.active_session_token == session_token:
                    return True, None
            elif desk.active_trader_email == clean_email:
                return True, None

            return False, f"Desk is actively controlled by {desk.active_trader_name or desk.active_trader_email}. Click 'Take Over Desk' to submit quotes."

    def record_quote_submission(self, assignment_id: str, email: str, price: float):
        """Mirrors latest submitted quote to all desk observers."""
        with self._lock:
            desk = self._get_or_create(assignment_id)
            desk.last_quote_price = price
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
        caller_key = session_token or caller_email
        other_spectators = [
            {"email": s["email"], "name": s["name"], "role": s.get("role", "EXECUTION")}
            for k, s in desk.spectators.items()
            if k != caller_key and not (desk.active_session_token and k == desk.active_session_token)
        ]

        active_info = None
        if desk.active_trader_email:
            is_you = False
            if session_token and desk.active_session_token:
                is_you = (desk.active_session_token == session_token)
            else:
                is_you = (desk.active_trader_email == caller_email)

            active_info = {
                "email": desk.active_trader_email,
                "name": desk.active_trader_name or desk.active_trader_email.split("@")[0],
                "active_since": desk.active_since.isoformat() if desk.active_since else None,
                "is_you": is_you
            }

        execution_colleagues = [s for s in other_spectators if s.get("role") == "EXECUTION"]

        return {
            "assignment_id": desk.assignment_id,
            "is_active_trader": is_active,
            "active_trader_email": desk.active_trader_email,
            "active_trader_name": desk.active_trader_name,
            "active_controller": active_info,
            "spectators_count": len(other_spectators) + (1 if (active_info and not active_info["is_you"]) else 0),
            "colleagues_online": other_spectators,
            "execution_colleagues_count": len(execution_colleagues),
            "superseded_trader": superseded_trader,
            "mirrored_quote": {
                "price": desk.last_quote_price,
                "by": desk.last_quote_by,
                "at": desk.last_quote_at.isoformat() if desk.last_quote_at else None
            }
        }


desk_session_service = DeskSessionService()
