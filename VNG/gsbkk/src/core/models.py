"""
Data models for ETL Engine configuration

Provides type-safe dataclasses for layout configuration.
Backward compatible with existing JSON layouts.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Literal


@dataclass
class InputSource:
    """Configuration for a single input source"""
    name: str
    type: Literal['file', 'jdbc', 'gsheet'] = 'file'
    # File source options
    path: Optional[str] = None
    format: Optional[str] = 'parquet'
    # JDBC source options
    connection: Optional[str] = None
    table: Optional[str] = None
    query: Optional[str] = None
    sqlTemplate: Optional[str] = None
    tableKey: Optional[str] = None
    properties: Optional[Dict[str, str]] = None
    # GSheet source options
    sheet_id: Optional[str] = None
    range: Optional[str] = None
    # Common options
    cache: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'InputSource':
        """Create InputSource from dictionary, ignoring unknown fields"""
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)


@dataclass
class OutputConfig:
    """Configuration for a single output destination"""
    type: Literal['file', 'jdbc', 'gsheet'] = 'file'
    # Common options
    name: Optional[str] = None
    mode: str = 'overwrite'
    # File output options
    path: Optional[str] = None
    format: Optional[str] = 'parquet'
    numPartitions: Optional[int] = None
    # JDBC output options
    connection: Optional[str] = None
    table: Optional[str] = None
    deleteCondition: Optional[str] = None
    properties: Optional[Dict[str, str]] = None
    add_updated_at: bool = False
    # GSheet output options
    sheet_id: Optional[str] = None
    range: Optional[str] = None
    columns: Optional[List[str]] = None
    sort_by: Optional[List[str]] = None
    update_mode: Optional[str] = None
    key_column: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OutputConfig':
        """Create OutputConfig from dictionary, ignoring unknown fields"""
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)


@dataclass
class LayoutConfig:
    """
    Main layout configuration
    
    Maps to JSON layout files with inputSources, sqlTemplate, and outputs.
    """
    inputSources: List[InputSource] = field(default_factory=list)
    outputs: List[OutputConfig] = field(default_factory=list)
    # Optional fields
    description: Optional[str] = None
    sqlTemplate: Optional[str] = None
    sparkConfig: Optional[Dict[str, str]] = None
    numPartitions: Optional[int] = None
    outputConnection: Optional[str] = None
    outputProperties: Optional[Dict[str, str]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LayoutConfig':
        """
        Create LayoutConfig from dictionary (parsed JSON)
        
        Handles nested inputSources and outputs arrays.
        """
        input_sources = [
            InputSource.from_dict(src) if isinstance(src, dict) else src
            for src in data.get('inputSources', [])
        ]
        
        outputs = [
            OutputConfig.from_dict(out) if isinstance(out, dict) else out
            for out in data.get('outputs', [])
        ]
        
        return cls(
            inputSources=input_sources,
            outputs=outputs,
            description=data.get('description'),
            sqlTemplate=data.get('sqlTemplate'),
            sparkConfig=data.get('sparkConfig'),
            numPartitions=data.get('numPartitions'),
            outputConnection=data.get('outputConnection'),
            outputProperties=data.get('outputProperties'),
        )


@dataclass
class JdbcCredentials:
    """JDBC connection credentials"""
    host: str
    port: int
    user: str
    password: str
    driver: str = 'org.postgresql.Driver'
    
    def get_url(self, database: str) -> str:
        """Build JDBC URL for given database"""
        return f"jdbc:postgresql://{self.host}:{self.port}/{database}"
    
    def get_properties(self) -> Dict[str, str]:
        """Get connection properties dict"""
        return {
            'user': self.user,
            'password': self.password,
            'driver': self.driver
        }
