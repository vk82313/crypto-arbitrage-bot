import os
import time
import threading
import requests
import random
from datetime import datetime, timedelta, timezone
from flask import Flask

app = Flask(__name__)

# ==================== CONFIGURATION ====================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Trading Parameters
PAPER_TRADING = os.getenv("PAPER_TRADING", "True").lower() == "true"
MAX_LOTS_PER_TRADE = int(os.getenv("MAX_LOTS_PER_TRADE", "10"))

ETH_PARAMS = {
    'max_premium': float(os.getenv("ETH_MAX_PREMIUM", "3.00")),
    'min_profit': float(os.getenv("ETH_MIN_PROFIT", "0.20")),
    'price_increment': float(os.getenv("ETH_PRICE_INCREMENT", "0.10"))
}

BTC_PARAMS = {
    'max_premium': float(os.getenv("BTC_MAX_PREMIUM", "20.00")),
    'min_profit': float(os.getenv("BTC_MIN_PROFIT", "3.00")),
    'price_increment': float(os.getenv("BTC_PRICE_INCREMENT", "1.00"))
}

# ==================== UTILITIES ====================
def get_ist_time():
    utc_now = datetime.now(timezone.utc)
    ist_offset = timedelta(hours=5, minutes=30)
    ist_time = utc_now + ist_offset
    return ist_time.strftime("%H:%M:%S")

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"📱 Telegram not configured: {message}")
        return
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            print(f"📱 Telegram sent")
        else:
            print(f"❌ Telegram error: {response.status_code}")
    except Exception as e:
        print(f"❌ Telegram failed: {e}")

class TimelineTracker:
    def __init__(self):
        self.timeline = []
    
    def add_step(self, action, emoji="📝"):
        timestamp = get_ist_time()
        self.timeline.append({
            'timestamp': timestamp,
            'action': action,
            'emoji': emoji
        })
    
    def get_timeline_text(self):
        return "\n".join([
            f"{step['emoji']} [{step['timestamp']}] {step['action']}"
            for step in self.timeline
        ])

# ==================== ARBITRAGE ENGINE ====================
class UltraFastArbitrageEngine:
    def __init__(self):
        self.eth_prices = self.generate_sample_eth_data()
        self.btc_prices = self.generate_sample_btc_data()
        self.cycle_count = 0
        self.start_time = time.time()
    
    def generate_sample_eth_data(self):
        """Generate realistic ETH options data"""
        return {
            'ETH-3500-C': {'symbol': 'ETH-3500-C', 'bid': 1.40, 'ask': 1.50, 'qty': 80},
            'ETH-3540-C': {'symbol': 'ETH-3540-C', 'bid': 0.80, 'ask': 0.90, 'qty': 36},
            'ETH-3560-C': {'symbol': 'ETH-3560-C', 'bid': 0.50, 'ask': 0.60, 'qty': 50},
            'ETH-3500-P': {'symbol': 'ETH-3500-P', 'bid': 0.90, 'ask': 1.00, 'qty': 45},
            'ETH-3540-P': {'symbol': 'ETH-3540-P', 'bid': 1.20, 'ask': 1.30, 'qty': 60},
            'ETH-3560-P': {'symbol': 'ETH-3560-P', 'bid': 1.50, 'ask': 1.60, 'qty': 30}
        }
    
    def generate_sample_btc_data(self):
        """Generate realistic BTC options data"""
        return {
            'BTC-50000-C': {'symbol': 'BTC-50000-C', 'bid': 16.50, 'ask': 16.80, 'qty': 45},
            'BTC-51000-C': {'symbol': 'BTC-51000-C', 'bid': 12.20, 'ask': 12.50, 'qty': 60},
            'BTC-52000-C': {'symbol': 'BTC-52000-C', 'bid': 8.80, 'ask': 9.10, 'qty': 30},
            'BTC-50000-P': {'symbol': 'BTC-50000-P', 'bid': 18.50, 'ask': 18.80, 'qty': 35},
            'BTC-51000-P': {'symbol': 'BTC-51000-P', 'bid': 15.20, 'ask': 15.50, 'qty': 50},
            'BTC-52000-P': {'symbol': 'BTC-52000-P', 'bid': 12.80, 'ask': 13.10, 'qty': 25}
        }
    
    def find_arbitrage_opportunities(self, asset, options_data):
        """Ultra-fast arbitrage detection"""
        opportunities = []
        strikes = self.group_options_by_strike(options_data)
        sorted_strikes = sorted(strikes.keys())
        
        for i in range(len(sorted_strikes) - 1):
            strike1 = sorted_strikes[i]
            strike2 = sorted_strikes[i + 1]
            
            # CALL arbitrage
            call_opp = self.check_call_arbitrage(asset, strikes, strike1, strike2)
            if call_opp:
                opportunities.append(call_opp)
            
            # PUT arbitrage
            put_opp = self.check_put_arbitrage(asset, strikes, strike1, strike2)
            if put_opp:
                opportunities.append(put_opp)
        
        return sorted(opportunities, key=lambda x: x['profit'], reverse=True)[:3]
    
    def check_call_arbitrage(self, asset, strikes, strike1, strike2):
        asset_params = ETH_PARAMS if asset == "ETH" else BTC_PARAMS
        
        call1_ask = strikes[strike1]['call'].get('ask', 0)
        call2_bid = strikes[strike2]['call'].get('bid', 0)
        
        if (call1_ask > 0 and call2_bid > 0 and 
            call1_ask <= asset_params['max_premium'] and 
            call2_bid <= asset_params['max_premium']):
            
            profit = call2_bid - call1_ask
            if profit >= asset_params['min_profit']:
                return {
                    'type': 'CALL',
                    'strike1': strike1,
                    'strike2': strike2,
                    'buy_premium': call1_ask,
                    'sell_premium': call2_bid,
                    'profit': profit,
                    'buy_symbol': strikes[strike1]['call']['symbol'],
                    'sell_symbol': strikes[strike2]['call']['symbol'],
                    'buy_qty': strikes[strike1]['call'].get('qty', 100),
                    'sell_qty': strikes[strike2]['call'].get('qty', 100)
                }
        return None
    
    def check_put_arbitrage(self, asset, strikes, strike1, strike2):
        asset_params = ETH_PARAMS if asset == "ETH" else BTC_PARAMS
        
        put1_bid = strikes[strike1]['put'].get('bid', 0)
        put2_ask = strikes[strike2]['put'].get('ask', 0)
        
        if (put1_bid > 0 and put2_ask > 0 and 
            put1_bid <= asset_params['max_premium'] and 
            put2_ask <= asset_params['max_premium']):
            
            profit = put1_bid - put2_ask
            if profit >= asset_params['min_profit']:
                return {
                    'type': 'PUT',
                    'strike1': strike1,
                    'strike2': strike2,
                    'buy_premium': put2_ask,
                    'sell_premium': put1_bid,
                    'profit': profit,
                    'buy_symbol': strikes[strike2]['put']['symbol'],
                    'sell_symbol': strikes[strike1]['put']['symbol'],
                    'buy_qty': strikes[strike2]['put'].get('qty', 100),
                    'sell_qty': strikes[strike1]['put'].get('qty', 100)
                }
        return None
    
    def group_options_by_strike(self, options_data):
        strikes = {}
        for symbol, data in options_data.items():
            strike = self.extract_strike(symbol)
            if strike > 0:
                if strike not in strikes:
                    strikes[strike] = {'call': {}, 'put': {}}
                
                if 'C-' in symbol:
                    strikes[strike]['call'] = data
                elif 'P-' in symbol:
                    strikes[strike]['put'] = data
        return strikes
    
    def extract_strike(self, symbol):
        try:
            parts = symbol.split('-')
            for part in parts:
                if part.isdigit() and len(part) > 2:
                    return int(part)
            return 0
        except:
            return 0

# ==================== ORDER EXECUTION WITH PARTIAL FILLS ====================
class UltraFastOrderExecutor:
    def __init__(self):
        self.active_trades = {}
    
    def execute_sell_with_partial_fill(self, symbol, price, quantity, asset):
        """Execute sell order with partial fill handling"""
        timeline = TimelineTracker()
        
        if PAPER_TRADING:
            # Simulate partial fills realistically
            filled_qty = random.choices(
                [quantity, quantity-1, quantity-2, quantity-3, int(quantity*0.7)],
                weights=[0.6, 0.1, 0.1, 0.1, 0.1]
            )[0]
            
            filled_qty = max(1, filled_qty)  # At least 1 lot filled
            
            timeline.add_step(f"SELL: {quantity} lots @ ${price:.2f}", "📝")
            
            if filled_qty == quantity:
                # Full fill
                timeline.add_step(f"SELL: {filled_qty} lots @ ${price:.2f}", "✅")
                print(f"📝 PAPER: SELL {filled_qty}/{quantity} {symbol} @ ${price:.2f} - FULL FILL")
            else:
                # Partial fill
                timeline.add_step(f"SELL: {filled_qty}/{quantity} lots @ ${price:.2f}", "✅")
                timeline.add_step(f"SELL: {quantity-filled_qty} lots CANCELLED", "❌")
                print(f"📝 PAPER: SELL {filled_qty}/{quantity} {symbol} @ ${price:.2f} - PARTIAL FILL")
            
            return filled_qty, timeline
        
        # Real trading implementation would go here
        return quantity, timeline
    
    def execute_buy_sequence(self, symbol, original_price, sell_price, quantity, asset):
        """Execute buy with price adjustments for exact filled quantity"""
        timeline = TimelineTracker()
        asset_params = ETH_PARAMS if asset == "ETH" else BTC_PARAMS
        
        current_price = original_price
        timeline.add_step(f"BUY: {quantity} lots @ ${current_price:.2f}", "📝")
        
        if PAPER_TRADING:
            # Realistic simulation
            scenario = random.choices(
                ['instant', 'adjustment', 'match_price', 'abandon'],
                weights=[0.4, 0.3, 0.2, 0.1]
            )[0]
            
            if scenario == 'instant':
                time.sleep(0.1)
                timeline.add_step(f"BUY: {quantity} lots @ ${current_price:.2f}", "✅")
                return True, current_price, timeline
            
            elif scenario == 'adjustment':
                time.sleep(0.2)
                current_price += asset_params['price_increment']
                timeline.add_step(f"BUY not filled → ${current_price:.2f}", "🔄")
                time.sleep(0.1)
                timeline.add_step(f"BUY: {quantity} lots @ ${current_price:.2f}", "✅")
                return True, current_price, timeline
            
            elif scenario == 'match_price':
                time.sleep(0.2)
                current_price += asset_params['price_increment']
                timeline.add_step(f"BUY not filled → ${current_price:.2f}", "🔄")
                time.sleep(0.2)
                current_price = sell_price
                timeline.add_step(f"BUY not filled → ${current_price:.2f} (sell price)", "🚀")
                time.sleep(0.1)
                timeline.add_step(f"BUY: {quantity} lots @ ${current_price:.2f}", "✅")
                return True, current_price, timeline
            
            else:  # abandon
                time.sleep(0.2)
                current_price += asset_params['price_increment']
                timeline.add_step(f"BUY not filled → ${current_price:.2f}", "🔄")
                time.sleep(0.2)
                current_price = sell_price
                timeline.add_step(f"BUY not filled → ${current_price:.2f} (sell price)", "🚀")
                time.sleep(0.1)
                timeline.add_step("BUY ABANDONED - Not filled", "❌")
                return False, current_price, timeline
        
        return True, original_price, timeline

    def execute_arbitrage_trade(self, asset, opportunity):
        """Complete trade execution with partial fill handling"""
        try:
            # Calculate initial trade quantity
            trade_qty = min(
                opportunity['buy_qty'], 
                opportunity['sell_qty'], 
                MAX_LOTS_PER_TRADE
            )
            
            if trade_qty < 1:
                return False
            
            emoji = "🔵" if asset == "ETH" else "🟡"
            asset_params = ETH_PARAMS if asset == "ETH" else BTC_PARAMS
            
            # Execute sell order with partial fill handling
            filled_qty, sell_timeline = self.execute_sell_with_partial_fill(
                opportunity['sell_symbol'], 
                opportunity['sell_premium'], 
                trade_qty, 
                asset
            )
            
            # If no lots filled, abort
            if filled_qty < 1:
                self.send_sell_timeout_alert(asset, opportunity, trade_qty, emoji)
                return False
            
            # If partial fill, adjust message
            if filled_qty < trade_qty:
                self.send_partial_fill_alert(asset, opportunity, trade_qty, filled_qty, emoji)
            
            # Execute buy sequence for EXACT filled quantity
            buy_success, final_price, buy_timeline = self.execute_buy_sequence(
                opportunity['buy_symbol'],
                opportunity['buy_premium'],
                opportunity['sell_premium'],
                filled_qty,  # ← Buy exactly what we sold
                asset
            )
            
            # Combine timelines
            combined_timeline = TimelineTracker()
            combined_timeline.timeline = sell_timeline.timeline + buy_timeline.timeline
            
            # Send result message
            self.send_trade_result_message(
                asset, opportunity, trade_qty, filled_qty, final_price, 
                combined_timeline, emoji, buy_success
            )
            
            return buy_success
            
        except Exception as e:
            error_msg = f"🚨 {asset} TRADE ERROR: {str(e)}"
            send_telegram(error_msg)
            print(f"{asset} Trade Error: {e}")
            return False

    def send_partial_fill_alert(self, asset, opportunity, ordered_qty, filled_qty, emoji):
        """Alert for partial fill"""
        message = f"""
⚠️ {asset} PARTIAL FILL

{emoji} {asset} {opportunity['type']} Spread
🔄 {opportunity['strike1']} → {opportunity['strike2']}
💰 Sell Price: ${opportunity['sell_premium']:.2f}

📊 ORDER: {ordered_qty} lots | FILLED: {filled_qty} lots | CANCELLED: {ordered_qty-filled_qty} lots
🔄 Adjusting buy quantity to {filled_qty} lots

🕒 Time: {get_ist_time()} IST
"""
        send_telegram(message)

    def send_trade_result_message(self, asset, opportunity, ordered_qty, filled_qty, final_price, timeline, emoji, success):
        """Send trade result to Telegram"""
        profit = opportunity['sell_premium'] - final_price
        total_pnl = profit * filled_qty
        
        if filled_qty < ordered_qty:
            fill_info = f" ({filled_qty}/{ordered_qty} filled)"
        else:
            fill_info = ""
        
        if success:
            status = "EXECUTED" if profit > 0 else "BREAK EVEN"
            message = f"""
🤖 {asset} TRADE {status}{fill_info}

{emoji} {asset} {opportunity['type']} Spread
🔄 {opportunity['strike1']} → {opportunity['strike2']}
💰 Buy: ${opportunity['buy_premium']:.2f} | Sell: ${opportunity['sell_premium']:.2f}
📦 Lots: {filled_qty} | Expected Profit: ${opportunity['profit']:.2f}

⏰ EXECUTION TIMELINE:
{timeline.get_timeline_text()}

💰 ACTUAL PROFIT: ${profit:.2f} per lot
💵 TOTAL P&L: ${total_pnl:.2f}

🕒 Completed: {get_ist_time()} IST
"""
        else:
            message = f"""
🚨 {asset} MANUAL INTERVENTION NEEDED{fill_info}

{emoji} {asset} {opportunity['type']} Spread
🔄 {opportunity['strike1']} → {opportunity['strike2']}
💰 Sold: {filled_qty} @ ${opportunity['sell_premium']:.2f} | Buy Attempted: ${final_price:.2f}

⏰ EXECUTION TIMELINE:
{timeline.get_timeline_text()}

🚨 CURRENT POSITION: {filled_qty} lots SHORT
👤 Please handle manually

🕒 Abandoned: {get_ist_time()} IST
"""
        
        send_telegram(message)

    def send_sell_timeout_alert(self, asset, opportunity, quantity, emoji):
        """Send sell timeout alert"""
        message = f"""
⏰ {asset} SELL ORDER TIMEOUT

{emoji} {asset} {opportunity['type']} Spread
🔄 {opportunity['strike1']} → {opportunity['strike2']}
💰 Attempted Sell: ${opportunity['sell_premium']:.2f}
📦 Quantity: {quantity} lots

❌ ACTION: No lots filled - Order cancelled
🤖 STATUS: Waiting for next opportunity

🕒 Time: {get_ist_time()} IST
"""
        send_telegram(message)

# ==================== ULTRA-FAST BOTS ====================
class UltraFastAPIBot:
    def __init__(self, asset):
        self.asset = asset
        self.arbitrage_engine = UltraFastArbitrageEngine()
        self.order_executor = UltraFastOrderExecutor()
        self.running = True
        self.cycle_count = 0
        self.start_time = time.time()
    
    def fetch_data(self):
        """Fetch data from API - ultra fast"""
        if self.asset == "ETH":
            return self.arbitrage_engine.eth_prices
        else:
            return self.arbitrage_engine.btc_prices
    
    def ultra_fast_monitoring(self):
        """Ultra-fast monitoring loop - no delays"""
        print(f"🚀 Starting Ultra-Fast {self.asset} Bot")
        
        while self.running:
            cycle_start = time.time()
            self.cycle_count += 1
            
            try:
                # 1. Fetch data (ultra fast)
                data = self.fetch_data()
                
                # 2. Find opportunities (ultra fast)
                opportunities = self.arbitrage_engine.find_arbitrage_opportunities(self.asset, data)
                
                # 3. Execute immediately if opportunities found
                if opportunities:
                    self.order_executor.execute_arbitrage_trade(self.asset, opportunities[0])
                
                # 4. Log speed every 500 cycles
                if self.cycle_count % 500 == 0:
                    elapsed = time.time() - self.start_time
                    cycles_per_second = self.cycle_count / elapsed
                    print(f"⚡ {self.asset}: {cycles_per_second:.1f} cycles/second")
                
                # 5. No sleep - immediate next cycle
                # Only tiny pause if needed to avoid 100% CPU
                elapsed_cycle = time.time() - cycle_start
                if elapsed_cycle < 0.05:  # If cycle too fast
                    time.sleep(0.02)  # 20ms tiny pause
                    
            except Exception as e:
                print(f"❌ {self.asset} Bot error: {e}")
                time.sleep(0.1)  # Brief pause on error

# ==================== FLASK ROUTES ====================
@app.route('/')
def home():
    return f"""
    <h1>🚀 Ultra-Fast Crypto Arbitrage Bot</h1>
    <p><strong>Status:</strong> Running - Ultra Fast Mode</p>
    <p><strong>Paper Trading:</strong> {PAPER_TRADING}</p>
    <p><strong>ETH:</strong> ${ETH_PARAMS['min_profit']} min profit</p>
    <p><strong>BTC:</strong> ${BTC_PARAMS['min_profit']} min profit</p>
    <p><strong>Polling:</strong> Maximum Speed (No Delays)</p>
    <p><strong>Features:</strong> Partial Fill Handling ✅</p>
    <p><a href="/health">Health Check</a></p>
    """

@app.route('/health')
def health():
    return {
        "status": "healthy",
        "mode": "ultra_fast",
        "paper_trading": PAPER_TRADING,
        "eth_min_profit": ETH_PARAMS['min_profit'],
        "btc_min_profit": BTC_PARAMS['min_profit'],
        "polling_speed": "maximum",
        "features": ["partial_fill_handling", "ultra_fast_api"],
        "timestamp": get_ist_time()
    }

# ==================== INITIALIZATION ====================
eth_bot = UltraFastAPIBot("ETH")
btc_bot = UltraFastAPIBot("BTC")

def start_ultra_fast_bots():
    """Start both bots in ultra-fast mode"""
    print("🚀 Starting Ultra-Fast Crypto Arbitrage Bot...")
    print(f"🔵 ETH: ${ETH_PARAMS['min_profit']} min profit, ${ETH_PARAMS['price_increment']} increments")
    print(f"🟡 BTC: ${BTC_PARAMS['min_profit']} min profit, ${BTC_PARAMS['price_increment']} increments")
    print(f"⚡ Polling: Maximum Speed (No Delays)")
    print(f"🔄 Features: Partial Fill Handling Enabled")
    print(f"📝 Paper Trading: {PAPER_TRADING}")
    
    # Start bots in separate threads
    eth_thread = threading.Thread(target=eth_bot.ultra_fast_monitoring, daemon=True)
    btc_thread = threading.Thread(target=btc_bot.ultra_fast_monitoring, daemon=True)
    
    eth_thread.start()
    btc_thread.start()
    
    send_telegram(f"🤖 Ultra-Fast Arbitrage Bot Started\n\n🔵 ETH: ${ETH_PARAMS['min_profit']} min profit\n🟡 BTC: ${BTC_PARAMS['min_profit']} min profit\n⚡ Polling: Maximum Speed\n🔄 Partial Fills: Enabled\n📝 Paper Trading: {PAPER_TRADING}")

if __name__ == "__main__":
    start_ultra_fast_bots()
    
    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 Starting Flask server on port {port}")
    
    app.run(
        host='0.0.0.0',
        port=port,
        debug=False,
        threaded=True
    )
