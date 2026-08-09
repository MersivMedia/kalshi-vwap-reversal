#!/usr/bin/env python3
"""
Kalshi API Client with rate limiting and retry logic.
"""

import os
import time
import json
import base64
import requests
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

# API endpoints
KALSHI_BASE_URL = 'https://api.elections.kalshi.com'


class RateLimitError(Exception):
    """Raised when rate limited by Kalshi."""
    pass


class KalshiClient:
    """
    Kalshi Perps API client with:
    - Request signing
    - Rate limit handling with exponential backoff
    - Retry logic for transient failures
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
        self._min_request_interval = 0.1  # 100ms between requests
    
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
    
    def _throttle(self):
        """Ensure minimum interval between requests."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()
    
    def _request(self, method: str, path: str, json_data: dict = None, 
                 retries: int = MAX_RETRIES) -> dict:
        """
        Make API request with retry and backoff.
        
        Args:
            method: HTTP method (GET, POST, DELETE)
            path: API path (e.g., '/trade-api/v2/margin/balance')
            json_data: Request body for POST
            retries: Number of retries remaining
            
        Returns:
            API response as dict
        """
        self._throttle()
        
        url = self.base_url + path
        headers = self._headers(method, path.split('?')[0])  # Sign without query params
        
        backoff = INITIAL_BACKOFF
        
        for attempt in range(retries + 1):
            try:
                if method == 'GET':
                    resp = requests.get(url, headers=headers, timeout=10)
                elif method == 'POST':
                    resp = requests.post(url, headers=headers, json=json_data, timeout=10)
                elif method == 'DELETE':
                    resp = requests.delete(url, headers=headers, timeout=10)
                else:
                    raise ValueError(f"Unknown method: {method}")
                
                # Handle rate limiting
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get('Retry-After', backoff))
                    print(f"[KALSHI] Rate limited, waiting {retry_after}s...")
                    time.sleep(retry_after)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                
                # Handle server errors with retry
                if resp.status_code >= 500:
                    if attempt < retries:
                        print(f"[KALSHI] Server error {resp.status_code}, retrying in {backoff}s...")
                        time.sleep(backoff)
                        backoff = min(backoff * 2, MAX_BACKOFF)
                        continue
                
                return resp.json()
                
            except requests.exceptions.Timeout:
                if attempt < retries:
                    print(f"[KALSHI] Timeout, retrying in {backoff}s...")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                return {'error': 'timeout'}
                
            except requests.exceptions.ConnectionError as e:
                if attempt < retries:
                    print(f"[KALSHI] Connection error, retrying in {backoff}s...")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                return {'error': str(e)}
                
            except Exception as e:
                return {'error': str(e)}
        
        return {'error': 'max retries exceeded'}
    
    def get_balance(self) -> float:
        """Get PERPS available balance (cash not in positions)."""
        data = self._request('GET', '/trade-api/v2/margin/balance')
        
        # Parse perps balance structure
        for sub in data.get('subaccount_balances', []):
            if sub.get('subaccount') == 0:
                available = float(sub.get('available_balance', 0))
                if available > 0:
                    return available
                equity = float(sub.get('account_equity', 0))
                if equity > 0:
                    return equity
        
        return float(data.get('settled_funds', 0))
    
    def get_equity(self) -> float:
        """Get total account equity (available + positions value)."""
        data = self._request('GET', '/trade-api/v2/margin/balance')
        
        for sub in data.get('subaccount_balances', []):
            if sub.get('subaccount') == 0:
                # account_equity = available_balance + margin_used + unrealized_pnl
                equity = float(sub.get('account_equity', 0))
                if equity > 0:
                    return equity
                # Fallback to available if no equity field
                return float(sub.get('available_balance', 0))
        
        return float(data.get('settled_funds', 0))
    
    def get_positions(self) -> List[dict]:
        """Get all open positions."""
        data = self._request('GET', '/trade-api/v2/margin/positions')
        
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
    
    def get_orderbook(self, ticker: str, depth: int = 5) -> dict:
        """Get orderbook for a perp market."""
        return self._request('GET', f'/trade-api/v2/margin/markets/{ticker}/orderbook?depth={depth}')
    
    def get_best_prices(self, ticker: str) -> Tuple[float, float]:
        """Get best bid and ask prices."""
        ob = self.get_orderbook(ticker)
        orderbook = ob.get('orderbook', {})
        bids = orderbook.get('bids', [])
        asks = orderbook.get('asks', [])
        
        best_bid = float(bids[0][0]) if bids else 0
        best_ask = float(asks[0][0]) if asks else 0
        
        return (best_bid, best_ask)
    
    def place_order(self, ticker: str, side: str, count: int, price: float,
                    reduce_only: bool = False, post_only: bool = True) -> dict:
        """
        Place limit order on Kalshi PERPS.
        
        Args:
            ticker: Market ticker (e.g., 'KXBTCPERP')
            side: 'long'/'buy' or 'short'/'sell'
            count: Number of contracts
            price: Price per contract
            reduce_only: If True, only reduces existing position
            post_only: If True, order rejected if it would take (default True for entries)
        """
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
        
        # Entry orders use post_only for maker fees
        if not reduce_only and post_only:
            order_data['post_only'] = True
        
        if reduce_only:
            order_data['reduce_only'] = True
        
        return self._request('POST', path, order_data)
    
    def cancel_order(self, order_id: str) -> dict:
        """Cancel an open order."""
        return self._request('DELETE', f'/trade-api/v2/margin/orders/{order_id}')
    
    def get_order(self, order_id: str) -> dict:
        """Get order status."""
        return self._request('GET', f'/trade-api/v2/margin/orders/{order_id}')
    
    def get_open_orders(self) -> List[dict]:
        """Get all open orders."""
        data = self._request('GET', '/trade-api/v2/margin/orders')
        return data.get('orders', [])
