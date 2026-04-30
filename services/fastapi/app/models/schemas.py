from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class EmissionReading(BaseModel):
    vehicle_id: str = Field(..., description="Fleet vehicle identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    nox_mg_km: float = Field(..., ge=0, description="NOx in mg/km")
    pm25_ug_m3: float = Field(..., ge=0, description="PM2.5 in µg/m³")
    speed_kmh: float = Field(..., ge=0)
    engine_temp_c: float
    fuel_rate_lh: float = Field(..., ge=0)
    lat: Optional[float] = None
    lon: Optional[float] = None


class AnomalyResult(BaseModel):
    vehicle_id: str
    timestamp: datetime
    nox_mg_km: float
    pm25_ug_m3: float
    is_anomaly: bool
    anomaly_score: float
    epa_tier: str
    conama_compliant: bool
