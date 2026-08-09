#!/usr/bin/env python3
"""
State Manager - Persist and recover bot state across restarts.

Saves:
- Exit targets (stop_loss, target_price) for positions
- Trade counts and PnL
"""

import json
import time
from pathlib import Path
from typing import Dict, Optional

STATE_FILE = Path(__file__).parent.parent / 'state' / 'bot_state.json'
STATE_FILE.parent.mkdir(exist_ok=True)

def save_state(exit_targets: Dict, trades_this_hour: int = 0, total_pnl: float = 0):
    """Save current bot state to disk."""
    state = {
        'saved_at': time.time(),
        'saved_at_iso': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'trades_this_hour': trades_this_hour,
        'total_pnl': total_pnl,
        'exit_targets': exit_targets
    }
    
    # Write atomically
    temp_file = STATE_FILE.with_suffix('.tmp')
    with open(temp_file, 'w') as f:
        json.dump(state, f, indent=2)
    temp_file.rename(STATE_FILE)
    
    return state

def load_state() -> Optional[Dict]:
    """Load saved state from disk."""
    if not STATE_FILE.exists():
        return None
    
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        
        # Check if state is stale (more than 1 hour old)
        age = time.time() - state.get('saved_at', 0)
        if age > 3600:  # 1 hour
            print(f"[STATE] Saved state is {age/60:.0f} minutes old, may be stale")
        
        return state
    except Exception as e:
        print(f"[STATE] Error loading state: {e}")
        return None

def clear_state():
    """Clear saved state."""
    if STATE_FILE.exists():
        STATE_FILE.unlink()
