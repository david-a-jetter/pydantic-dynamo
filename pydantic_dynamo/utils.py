from datetime import datetime, timezone
from typing import Optional, Sequence, Iterable


def get_error_code(ex: Exception) -> Optional[str]:
    if hasattr(ex, "response"):
        return ex.response.get("Error", {}).get("Code")  # type: ignore[no-any-return,attr-defined]
    return None


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def chunks(items: Sequence, size: int) -> Iterable[Sequence]:
    """Yield successive n-sized chunks from items."""
    for i in range(0, len(items), size):
        yield items[i : (i + size)]
