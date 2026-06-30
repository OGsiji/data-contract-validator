"""
Canonical type system.

Every extractor normalizes its native types (warehouse SQL types, Python type
hints, etc.) into a small, framework-neutral vocabulary. The validator then
compares *canonical* types instead of raw strings.

This is the change that kills the false-positive noise: a dbt ``varchar`` and a
Pydantic ``str`` both become ``CanonicalType.STRING``, so they no longer look
like a mismatch. When we genuinely cannot determine a type we return
``CanonicalType.UNKNOWN``, which is treated as compatible with everything --
the tool stays quiet rather than crying wolf.
"""

from enum import Enum
import re
from typing import Optional


class CanonicalType(Enum):
    """Framework-neutral type vocabulary."""

    STRING = "string"
    INTEGER = "integer"  # 32-bit-ish integers
    BIGINT = "bigint"  # 64-bit integers
    DECIMAL = "decimal"  # fixed-precision numeric
    FLOAT = "float"  # floating point (real/double)
    BOOLEAN = "boolean"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    JSON = "json"  # semi-structured / object / map
    ARRAY = "array"
    STRUCT = "struct"
    BINARY = "binary"
    UUID = "uuid"
    UNKNOWN = "unknown"  # could not determine -> compatible with anything


# --- Warehouse / SQL type names -> canonical -------------------------------

_SQL_TYPE_MAP = {
    # strings
    "varchar": CanonicalType.STRING,
    "nvarchar": CanonicalType.STRING,
    "char": CanonicalType.STRING,
    "nchar": CanonicalType.STRING,
    "character": CanonicalType.STRING,
    "character varying": CanonicalType.STRING,
    "string": CanonicalType.STRING,
    "text": CanonicalType.STRING,
    "ntext": CanonicalType.STRING,
    # small / regular integers
    "int": CanonicalType.INTEGER,
    "integer": CanonicalType.INTEGER,
    "int2": CanonicalType.INTEGER,
    "int4": CanonicalType.INTEGER,
    "smallint": CanonicalType.INTEGER,
    "tinyint": CanonicalType.INTEGER,
    "mediumint": CanonicalType.INTEGER,
    "byteint": CanonicalType.INTEGER,
    # big integers
    "bigint": CanonicalType.BIGINT,
    "int8": CanonicalType.BIGINT,
    "int64": CanonicalType.BIGINT,  # BigQuery
    "long": CanonicalType.BIGINT,
    # fixed-precision numerics
    "number": CanonicalType.DECIMAL,  # Snowflake/Oracle (refined by scale above)
    "numeric": CanonicalType.DECIMAL,
    "decimal": CanonicalType.DECIMAL,
    "bignumeric": CanonicalType.DECIMAL,
    "money": CanonicalType.DECIMAL,
    # floating point
    "float": CanonicalType.FLOAT,
    "float4": CanonicalType.FLOAT,
    "float8": CanonicalType.FLOAT,
    "float64": CanonicalType.FLOAT,  # BigQuery
    "double": CanonicalType.FLOAT,
    "double precision": CanonicalType.FLOAT,
    "real": CanonicalType.FLOAT,
    # boolean
    "boolean": CanonicalType.BOOLEAN,
    "bool": CanonicalType.BOOLEAN,
    "bit": CanonicalType.BOOLEAN,
    # date / time
    "date": CanonicalType.DATE,
    "time": CanonicalType.TIME,
    "datetime": CanonicalType.TIMESTAMP,
    "datetime2": CanonicalType.TIMESTAMP,
    "smalldatetime": CanonicalType.TIMESTAMP,
    "timestamp": CanonicalType.TIMESTAMP,
    "timestamp_ntz": CanonicalType.TIMESTAMP,
    "timestamp_tz": CanonicalType.TIMESTAMP,
    "timestamp_ltz": CanonicalType.TIMESTAMP,
    "timestamptz": CanonicalType.TIMESTAMP,
    "datetimeoffset": CanonicalType.TIMESTAMP,
    # semi-structured
    "json": CanonicalType.JSON,
    "jsonb": CanonicalType.JSON,
    "variant": CanonicalType.JSON,  # Snowflake
    "object": CanonicalType.JSON,
    "map": CanonicalType.JSON,
    "super": CanonicalType.JSON,  # Redshift
    # array / struct
    "array": CanonicalType.ARRAY,
    "struct": CanonicalType.STRUCT,
    "record": CanonicalType.STRUCT,  # BigQuery
    "row": CanonicalType.STRUCT,
    # binary
    "binary": CanonicalType.BINARY,
    "varbinary": CanonicalType.BINARY,
    "bytes": CanonicalType.BINARY,  # BigQuery
    "bytea": CanonicalType.BINARY,  # Postgres
    "blob": CanonicalType.BINARY,
    # uuid
    "uuid": CanonicalType.UUID,
    "uniqueidentifier": CanonicalType.UUID,
}


def normalize_sql_type(raw_type: Optional[str]) -> CanonicalType:
    """Normalize a warehouse/SQL type name into a :class:`CanonicalType`.

    Handles parameterized types (``VARCHAR(255)``, ``NUMBER(38,0)``,
    ``DECIMAL(10,2)``) and the Snowflake/Oracle convention where
    ``NUMBER(p, 0)`` (scale 0) is really an integer.
    """
    if not raw_type:
        return CanonicalType.UNKNOWN

    text = raw_type.strip().lower()
    if not text:
        return CanonicalType.UNKNOWN

    # Pull out precision/scale, e.g. number(38,0) -> base "number", args [38, 0]
    base = text
    args = []
    paren = re.search(r"\(([^)]*)\)", text)
    if paren:
        base = text[: paren.start()].strip()
        args = [a.strip() for a in paren.group(1).split(",") if a.strip()]

    # Strip trailing modifiers like "varchar without time zone" is handled via map;
    # also collapse internal whitespace for multi-word names.
    base = re.sub(r"\s+", " ", base).strip()

    # NUMBER/NUMERIC/DECIMAL with scale 0 behaves as an integer.
    if base in ("number", "numeric", "decimal"):
        if len(args) >= 2 and args[1] == "0":
            precision = int(args[0]) if args[0].isdigit() else 38
            return CanonicalType.BIGINT if precision > 9 else CanonicalType.INTEGER
        if len(args) == 1 and args[0].isdigit():
            # NUMBER(38) with no scale -> integer family in most warehouses
            return CanonicalType.BIGINT if int(args[0]) > 9 else CanonicalType.INTEGER

    # Array/struct/map can appear as "array<...>", "struct<...>" -> match base word
    base_word = base.split("<")[0].strip()

    return _SQL_TYPE_MAP.get(base, _SQL_TYPE_MAP.get(base_word, CanonicalType.UNKNOWN))


# --- Python type hints -> canonical ----------------------------------------

_PYTHON_TYPE_MAP = {
    "str": CanonicalType.STRING,
    "int": CanonicalType.INTEGER,
    "float": CanonicalType.FLOAT,
    "complex": CanonicalType.FLOAT,
    "bool": CanonicalType.BOOLEAN,
    "bytes": CanonicalType.BINARY,
    "bytearray": CanonicalType.BINARY,
    "datetime": CanonicalType.TIMESTAMP,
    "date": CanonicalType.DATE,
    "time": CanonicalType.TIME,
    "decimal": CanonicalType.DECIMAL,
    "uuid": CanonicalType.UUID,
    "dict": CanonicalType.JSON,
    "mapping": CanonicalType.JSON,
    "list": CanonicalType.ARRAY,
    "tuple": CanonicalType.ARRAY,
    "set": CanonicalType.ARRAY,
    "frozenset": CanonicalType.ARRAY,
    "sequence": CanonicalType.ARRAY,
    "any": CanonicalType.UNKNOWN,
    "none": CanonicalType.UNKNOWN,
}


def normalize_python_type(raw_type: Optional[str]) -> CanonicalType:
    """Normalize a Python type-hint string into a :class:`CanonicalType`.

    Accepts strings as produced by the FastAPI extractor's AST walker, e.g.
    ``"str"``, ``"Optional[int]"``, ``"List[Item]"``, ``"datetime.datetime"``.
    Unknown/custom classes (nested models, enums) resolve to ``UNKNOWN`` so we
    never raise a false mismatch on a type we don't actually understand.
    """
    if not raw_type:
        return CanonicalType.UNKNOWN

    text = raw_type.strip().lower()
    if not text or text == "unknown":
        return CanonicalType.UNKNOWN

    # Unwrap Optional[...] / Union[..., none] to the meaningful inner type.
    opt = re.match(r"^(optional|union)\[(.+)\]$", text)
    if opt:
        inner = opt.group(2)
        # For Union, drop a trailing ", none" and take the first real member.
        inner = inner.split(",")[0].strip()
        return normalize_python_type(inner)

    # Container generics.
    container = re.match(r"^(\w+)\[(.+)\]$", text)
    if container:
        outer = container.group(1)
        if outer in ("list", "tuple", "set", "frozenset", "sequence"):
            return CanonicalType.ARRAY
        if outer in ("dict", "mapping"):
            return CanonicalType.JSON
        # e.g. some Annotated/custom generic -> fall through on outer name

    # Qualified names: datetime.datetime, uuid.UUID, decimal.Decimal.
    leaf = text.rsplit(".", 1)[-1]

    return _PYTHON_TYPE_MAP.get(text, _PYTHON_TYPE_MAP.get(leaf, CanonicalType.UNKNOWN))


# --- Compatibility ----------------------------------------------------------

# Numeric widening rank: a producer value of rank R can be safely consumed by a
# target expecting rank >= R (integer fits in bigint fits in decimal/float).
_NUMERIC_RANK = {
    CanonicalType.INTEGER: 1,
    CanonicalType.BIGINT: 2,
    CanonicalType.DECIMAL: 3,
    CanonicalType.FLOAT: 3,
}

# Pairs that are considered safe beyond exact match / numeric widening.
# Direction is (source_produces, target_expects).
_EXTRA_COMPATIBLE = {
    # UUIDs are routinely serialized as strings on the wire, both directions.
    (CanonicalType.UUID, CanonicalType.STRING),
    (CanonicalType.STRING, CanonicalType.UUID),
    # A DATE can be safely consumed where a TIMESTAMP is expected (widening).
    (CanonicalType.DATE, CanonicalType.TIMESTAMP),
    # Structured types flow into a JSON target.
    (CanonicalType.STRUCT, CanonicalType.JSON),
    (CanonicalType.ARRAY, CanonicalType.JSON),
    (CanonicalType.JSON, CanonicalType.STRUCT),
}


def types_compatible(source: CanonicalType, target: CanonicalType) -> bool:
    """Return True if a value of ``source`` can be safely consumed as ``target``.

    ``source`` is what the data pipeline produces; ``target`` is what the API
    expects. Unknowns are always compatible (we don't flag what we can't
    determine) -- this is deliberate to keep false positives at zero.
    """
    if source == CanonicalType.UNKNOWN or target == CanonicalType.UNKNOWN:
        return True

    if source == target:
        return True

    # Numeric widening.
    if source in _NUMERIC_RANK and target in _NUMERIC_RANK:
        return _NUMERIC_RANK[target] >= _NUMERIC_RANK[source]

    return (source, target) in _EXTRA_COMPATIBLE


# --- Column name normalization ---------------------------------------------


def normalize_name(name: Optional[str]) -> str:
    """Canonicalize a table/column name for matching.

    Lowercases, trims, and collapses common boundary noise so that
    ``userId``, ``user_id`` and ``USER_ID`` all match. Snake/camel are folded
    by stripping underscores after a camel-case split.
    """
    if not name:
        return ""

    text = name.strip()
    # Split camelCase / PascalCase into words, then join with underscores.
    text = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    return text.lower().strip()
