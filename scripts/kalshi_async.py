#!/usr/bin/env python3
"""
Async Kalshi API Client using aiohttp.

Non-blocking API calls for use within async event loop.
Includes rate limiting and exponential backoff.
"""

import os
import time
import asyncio
import base64
import aiohttp
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

# Rate limit settings
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 30.0
MIN_REQUEST_INTERVAL = 0.1  # 100ms between requests

# API endpoints
KALSHI_BASE_URL = 'https://api.elections.kalshi.com'


class KalshiAsyncClient:
    """
    Async Kalshi Perps API client with:
    - Request signing
    - Rate limit handling with exponential backoff
    - Non-blocking async requests
    """
    
    def __init__(self, api_key: str = None, key_path: str = None):
        self.api_key = api_key or os.getenv('KALSHI_API_KEY_ID')
        self.key_path = key_path or os.getenv('KALSHI_KEY_PATH', 'keys/kalshi_private.pem')
        self.base_url = KALSHI_BASE_URL
        
        # Resolve relative key path
        if not Path(self.key_path).is_absolute():
            self.key_path = Path(__file__).parent.parent / self.key_path
        
        # Load private key
        with open(self.key_path, 'rb') as f:
            self.private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
        
        # Rate limit tracking
        self._last_request_time = 0.0
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session
    
    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    def _sign(self, ts: str, method: str, path: str) -> str:
        """Sign request for Kalshi API."""
        msg = f"{ts}{method}{path}".encode('utf-8')
        signature = self.private_key.sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')
    
    def _headers(self, method: str, path: str) -> dict:
        """Generate signed headers."""
        ts = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        return {
            'KALSHI-ACCESS-KEY': self.api_key,
            'KALSHI-ACCESS-SIGNATURE': self._sign(ts, method, path),
            'KALSHI-ACCESS-TIMESTAMP': ts,
            'Content-Type': 'application/json'
        }
    
    async def _throttle(self):
        """Ensure minimum interval between requests."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            await asyncio.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()
    
    async def _request(self, method: str, path: str, json_data: dict = None,
                       retries: int = MAX_RETRIES) -> dict:
        """
        Make async API request with retry and backoff.
        """
        await self._throttle()
        
        session = await self._get_session()
        url = self.base_url + path
        headers = self._headers(method, path.split('?')[0])
        
        backoff = INITIAL_BACKOFF
        
        for attempt in range(retries + 1):
            try:
                if method == 'GET':
                    async with session.get(url, headers=headers) as resp:
                        status = resp.status
                        data = await resp.json()
                elif method == 'POST':
                    async with session.post(url, headers=headers, json=json_data) as resp:
                        status = resp.status
                        data = await resp.json()
                elif method == 'DELETE':
                    async with session.delete(url, headers=headers) as resp:
                        status = resp.status
                        data = await resp.json()
                else:
                    raise ValueError(f"Unknown method: {method}")
                
                # Handle rate limiting
                if status == 429:
                    retry_after = int(resp.headers.get('Retry-After', backoff))
                    print(f"[KALSHI] Rate limited, waiting {retry_after}s...")
                    await asyncio.sleep(retry_after)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                
                # Handle server errors with retry
                if status >= 500:
                    if attempt < retries:
                        print(f"[KALSHI] Server error {status}, retrying in {backoff}s...")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, MAX_BACKOFF)
                        continue
                
                return data
                
            except asyncio.TimeoutError:
                if attempt < retries:
                    print(f"[KALSHI] Timeout, retrying in {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                return {'error': 'timeout'}
                
            except aiohttp.ClientError as e:
                if attempt < retries:
                    print(f"[KALSHI] Client error, retrying in {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                return {'error': str(e)}
                
            except Exception as e:
                return {'error': str(e)}
        
        return {'error': 'max retries exceeded'}
    
    async def get_balance(self) -> float:
        """Get PERPS margin balance."""
        data = await self._request('GET', '/trade-api/v2/margin/balance')
        
        for sub in data.get('subaccount_balances', []):
            if sub.get('subaccount') == 0:
                available = float(sub.get('available_balance', 0))
                if available > 0:
                    return available
                equity = float(sub.get('account_equity', 0))
                if equity > 0:
                    return equity
        
        return float(data.get('settled_funds', 0))
    
    async def get_positions(self) -> List[dict]:
        """Get all open positions."""
        data = await self._request('GET', '/trade-api/v2/margin/positions')
        
        positions = []
        for p in data.get('positions', []):
            pos_size = float(p.get('position', 0))
            if pos_size != 0:
                positions.append({
                    'ticker': p.get('market_ticker', ''),
                    'size': pos_size,
                    'contracts': abs(int(pos_size)),
                    'side': 'long' if pos_size > 0 else 'short',
                    'entry_price': float(p.get('average_entry_price', 0)),
                    'unrealized_pnl': float(p.get('unrealized_pnl', 0))
                })
        
        return positions
    
    async def get_orderbook(self, ticker: str, depth: int = 5) -> dict:
        """Get orderbook for a perp market."""
        return await self._request('GET', f'/trade-api/v2/margin/markets/{ticker}/orderbook?depth={depth}')
    
    async def get_best_prices(self, ticker: str) -> Tuple[float, float]:
        """Get best bid and ask prices."""
        ob = await self.get_orderbook(ticker)
        orderbook = ob.get('orderbook', {})
        bids = orderbook.get('bids', [])
        asks = orderbook.get('asks', [])
        
        best_bid = float(bids[0][0]) if bids else 0
        best_ask = float(asks[0][0]) if asks else 0
        
        return (best_bid, best_ask)
    
    async def place_order(self, ticker: str, side: str, count: int, price: float,
                          reduce_only: bool = False, post_only: bool = True) -> dict:
        """Place limit order on Kalshi PERPS."""
        import uuid
        
        path = '/trade-api/v2/margin/orders'
        api_side = 'bid' if side.lower() in ('long', 'buy', 'bid') else 'ask'
        tif = 'immediate_or_cancel' if reduce_only else 'good_till_canceled'
        
        order_data = {
            'ticker': ticker,
            'client_order_id': str(uuid.uuid4()),
            'side': api_side,
            'count': str(int(count)),
            'price': f'{price:.4f}',
            'time_in_force': tif,
            'self_trade_prevention_type': 'taker_at_cross'
        }
        
        if not reduce_only and post_only:
            order_data['post_only'] = True
        
        if reduce_only:
            order_data['reduce_only'] = True
        
        return await self._request('POST', path, order_data)
    
    async def cancel_order(self, order_id: str) -> dict:
        """Cancel an open order."""
        return await self._request('DELETE', f'/trade-api/v2/margin/orders/{order_id}')
    
    async def get_order(self, order_id: str) -> dict:
        """Get order status."""
        return await self._request('GET', f'/trade-api/v2/margin/orders/{order_id}')
    
    async def get_open_orders(self) -> List[dict]:
        """Get all open orders."""
        data = await self._request('GET', '/trade-api/v2/margin/orders')
        return data.get('orders', [])
