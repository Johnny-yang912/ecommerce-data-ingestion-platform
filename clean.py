from typing import Dict, Any

def clean_and_validate(payload: Dict[str, Any]) -> Dict[str, Any]:
    name_raw = payload.get("name", "")
    name = str(name_raw).strip() if name_raw is not None else ""
    if not name:
        raise ValueError("Name is required and cannot be empty.")
    value_raw = payload.get("value", None)
    if value_raw is None:
        value = None
    else:
        try:
            value = float(value_raw)
        except (ValueError, TypeError):
            raise ValueError("Value must be a number.")
    return {"name": name, "value": value}