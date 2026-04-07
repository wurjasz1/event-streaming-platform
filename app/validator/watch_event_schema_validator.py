import json
from pathlib import Path
from jsonschema import validate as json_validate ,ValidationError

#producer-side python validator
#it reads schema from schemas directory compare it against json and validate
class SchemaValidator:
    def __init__(self, schema_path: str):
        self.schema=json.loads(Path(schema_path).read_text())

    def validate(self,payload:dict)->None:
        try:
            json_validate(instance=payload,schema=self.schema)
        except ValidationError as err:
            raise ValueError(f"Schema validation failed: {err.message}") from err