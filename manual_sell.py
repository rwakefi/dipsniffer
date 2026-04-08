import ccxt
import json
import os
import time
from datetime import datetime, timezone

KRAKEN_API_KEY = "Z/LSUJZOd0zORYKqW45WZMzeP+lushLYPocIOlN8eOV9XhkO2C7x5wkF"
KRAKEN_API_SECRET = "4Me3xMi5iBOU7SA9WOOgOI0uJ4U70CX1rpySgtOHqG0hCkQQG+EB5A8mDGkDZ8ue2bu60aWxvqNXhIS0JtDDhg=="
STATE_FILE = os.path.expanduser("~/.config/dipsniffer/swing-bot-state.json")

def sell_sui():
    exchange = ccxt.kraken({
        'apiKey': KRAKEN_API_KEY,
        'secret': KRAKEN_API_SECRET,
        'enableRateLimit': True,
    })
    exchange.nonce = lambda: int(time.time() * 1000000)
    
    with open(STATE_FILE, 'r') as f:
        state = json.load(f)
    
    if state.get("position") != "SUI":
        print(f"Error: Position is {state.get('position')}, not SUI.")
        return

    symbol = "SUI/USD"
    qty = state["quantity"]
    
    print(f"Executing MARKET SELL of {qty} {symbol}...")
    order = exchange.create_market_sell_order(symbol, qty)
    print(f"Order ID: {order['id']}")
    
    # Wait for fill confirmation
    time.sleep(2)
    order_info = exchange.fetch_order(order['id'], symbol)
    sell_price = order_info.get('average') or order_info.get('price')
    sell_qty = order_info.get('filled') or qty
    fee = order_info.get('fee', {}).get('cost', 0)
    
    if not sell_price:
        ticker = exchange.fetch_ticker(symbol)
        sell_price = ticker['last']
    
    sell_time = datetime.now(timezone.utc).isoformat()
    pnl = (sell_price - state["entry_price"]) * sell_qty
    pnl_pct = ((sell_price / state["entry_price"]) - 1) * 100
    
    trade_entry = {
        "action": "SELL",
        "symbol": "SUI",
        "price": round(sell_price, 4),
        "quantity": round(sell_qty, 5),
        "time": sell_time,
        "reason": "MANUAL SELL (User Request)",
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl_pct, 2),
        "fee": round(fee, 5)
    }
    
    state["trades"].append(trade_entry)
    state["position"] = None
    state["entry_price"] = 0
    state["entry_time"] = None
    state["quantity"] = 0
    state["stop_loss"] = 0
    state["highest_since_entry"] = 0
    state["highest_time"] = None
    state["last_sell_time"] = sell_time
    state["total_pnl"] += pnl
    
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)
    
    print(f"✅ SUI sold at {sell_price}. Bot state updated to CASH.")

if __name__ == "__main__":
    sell_sui()
