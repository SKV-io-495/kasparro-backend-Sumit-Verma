import os
from typing import List, Tuple, Any
from datetime import datetime, timezone
from pycoingecko import CoinGeckoAPI
from tenacity import retry, stop_after_attempt, wait_exponential

from app.schemas.crypto import CryptoUnifiedData
from app.core.logging_config import get_logger

logger = get_logger("etl_coingecko")

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
# Using tenacity here to handle CoinGecko's aggressive 429 rate limits without crashing the pipeline.
# Retries with exponential backoff ensure we respect the API provider while maintaining data freshness.
def fetch_data() -> Tuple[Any, List[CryptoUnifiedData]]:
    """
    Fetches market data from CoinGecko and normalizes it.
    Returns (raw_payload, normalized_data)
    """
    api_key = os.getenv("COINGECKO_API_KEY")
    
    # Initialize client with API key if available
    if api_key:
        cg = CoinGeckoAPI(api_key=api_key)
    else:
        cg = CoinGeckoAPI()
    
    try:
        # Fetch market data (vs_currency='usd')
        # This endpoint returns a list of coins with market data
        markets = cg.get_coins_markets(vs_currency='usd')
        
        results = []
        for m in markets:
            try:
                # Mapping:
                # symbol -> ticker
                # current_price -> price_usd
                # market_cap -> market_cap
                # total_volume -> volume_24h
                # last_updated -> timestamp
                
                price = m.get('current_price')
                if price is None:
                    continue
                
                ts_str = m.get('last_updated')
                if ts_str:
                    try:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    except ValueError:
                        ts = datetime.now(timezone.utc)
                else:
                    ts = datetime.now(timezone.utc)
                
                if not m.get('symbol'):
                    continue

                data = CryptoUnifiedData(
                    ticker=m.get('symbol'),
                    price_usd=float(price),
                    market_cap=m.get('market_cap'),
                    volume_24h=m.get('total_volume'),
                    source="coingecko",
                    timestamp=ts
                )
                results.append(data)
            except Exception as e:
                logger.warning("conversion_error", source="coingecko", ticker=m.get('symbol', 'unknown'), error=str(e))
                continue
                
        return markets, results

    except Exception as e:
        logger.error("fetch_error", source="coingecko", error=str(e))
        raise e
