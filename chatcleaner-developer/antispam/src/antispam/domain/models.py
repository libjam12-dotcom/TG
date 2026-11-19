from dataclasses import dataclass
from enum import StrEnum


class Action(StrEnum):
    ALLOW = "allow"
    REVIEW = "review"
    DELETE = "delete"
    RESTRICT = "restrict"
    BAN = "ban"


@dataclass(frozen=True)
class MessageContext:
    chat_id: int
    message_id: int
    user_id: int | None
    text: str
    observed_at: int
    thread_id: int | None = None
    sender_chat_id: int | None = None
    joined_at: int | None = None
    first_messages_seen: int = 0
    is_edit: bool = False
    is_manual_check: bool = False
    links: tuple[str, ...] = ()
    protected: bool = False


@dataclass(frozen=True)
class Signal:
    code: str
    weight: int
    rule_id: int | None = None


@dataclass(frozen=True)
class Decision:
    action: Action
    signals: tuple[Signal, ...]
    policy_version: int
    needs_admin: bool = False
    category: str | None = None
