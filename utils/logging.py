from datetime import date, datetime
import json

def safe_json_context(context_dict):
    def convert(value):
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        return value
    return json.dumps({k: convert(v) for k, v in context_dict.items()})
