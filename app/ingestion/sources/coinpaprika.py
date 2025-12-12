from typing import List
from datetime import datetime, timezone
from coinpaprika import client as Coinpaprika
from tenacity import retry, stop_after_attempt, wait_exponential

from app.schemas.crypto import CryptoUnifiedData
from app.core.logging_config import get_logger

logger = get_logger("etl_coinpaprika")

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def fetch_data() -> List[CryptoUnifiedData]:
    """
    Fetches top 50 coins from CoinPaprika and normalizes them.
    """
    client = Coinpaprika.Client()
    
    try:
        # fetch tickers
        tickers = client.tickers()
        
        # Filter top 50
        top_50 = tickers[:50]
        
        results = []
        for t in top_50:
            try:
                # CoinPaprika tickers() response items usually have 'quotes' dictionary
                # e.g. t['quotes']['USD']['price']
                # But client.tickers() returns list of dicts.
                # Let's verify structure assumption. 
                # According to docs/standard usage:
                # t['symbol'] -> ticker
                # t['quotes']['USD']['price'] -> price_usd
                # t['quotes']['USD']['market_cap'] -> market_cap
                # t['quotes']['USD']['volume_24h'] -> volume_24h
                # t['last_updated'] -> timestamp (ISO string)
                
                usd_quote = t.get('quotes', {}).get('USD', {})
                
                price = usd_quote.get('price')
                if price is None:
                    continue # Skip if no price
                    
                ts_str = t.get('last_updated')
                if ts_str:
                    try:
                        # CoinPaprika often returns ISO format like '2022-05-16T18:24:28Z'
                        # It might handle 'Z' differently in different python versions if fromisoformat is used
                        # But mostly it works.
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    except ValueError:
                         ts = datetime.now(timezone.utc)
                else:
                    ts = datetime.now(timezone.utc)

                data = CryptoUnifiedData(
                    ticker=t.get('symbol'),
                    price_usd=price,
                    market_cap=usd_quote.get('market_cap'),
                    volume_24h=usd_quote.get('volume_24h'),
                    source="coinpaprika",
                    timestamp=ts
                )
                results.append(data)
            except Exception as e:
                logger.warning("conversion_error", source="coinpaprika", ticker=t.get('symbol', 'unknown'), error=str(e))
                continue
                
        return results

    except Exception as e:
        logger.error("fetch_error", source="coinpaprika", error=str(e))
        raise e
