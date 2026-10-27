from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class AnimeMappingDecision:
    outcome: str
    mal_ids: tuple[str, ...] = ()
    evidence: list[dict[str, str]] = field(default_factory=list)

    @property
    def mal_id(self) -> str | None:
        return self.mal_ids[0] if self.outcome == "matched" else None
