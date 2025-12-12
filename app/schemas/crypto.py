"""
Defines the Unified Crypto Schema used across all data sources.
Ensures that data from CoinGecko, CoinPaprika, etc. matches a strict contract before hitting the DB.
"""
from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime

class CryptoUnifiedData(BaseModel):
    ticker: str = Field(..., description="e.g., BTC, ETH")
    price_usd: float = Field(..., description="Price in USD")
    market_cap: Optional[float] = Field(None, description="Market Capitalization")
    volume_24h: Optional[float] = Field(None, description="24h Trading Volume")
    source: str = Field(..., description="Source of data e.g. coinpaprika, coingecko")
    timestamp: datetime = Field(..., description="Timestamp of the data")

    @validator('ticker')
    def uppercase_ticker(cls, v):
        return v.upper()

    class Config:
        from_attributes = True

class CryptoUnifiedDataResponse(CryptoUnifiedData):
    id: int
