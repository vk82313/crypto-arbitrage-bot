import os
import time
import threading
import requests
import random
import traceback
import sys
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

# ==================== GLOBAL ERROR HANDLER ====================
class GlobalErrorHandler:
    def __init__(self):
        self.is_shutdown = False
        self.bots = []
    
    def register_bot(self, bot):
        self.bots.append(bot)
    
    def global_exception_handler(self, exc_type, exc_value, exc_traceback):
        """Global exception handler that stops everything and sends alert"""
        if self.is_shutdown:
            return
        
        self.is_shutdown = True
        
        # Get detailed error information
        error_time = get_ist_time()
        error_details = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        
        # Create comprehensive error message
        error_message = f"""
🚨🚨🚨 CRITICAL BOT FAILURE 🚨🚨🚨

⏰ Time: {error_time} IST
🔴 Status: ALL BOTS STOPPED
💥 Error Type: {exc_type.__name__}

📋 Error Details:
{str(exc_value)}

🔧 Stack Trace:
{error_details[:1500]}...  # Truncate if too long

🛑 ACTION REQUIRED:
• All trading has been STOPPED
• Manual restart required
• Check PythonAnywhere logs
• Verify API connectivity

⚠️ DO NOT IGNORE - Trading is completely halted!
"""
        
        # Send immediate critical alert
        self.send_critical_alert(error_message)
        
        # Stop all bots
        self.stop_all_bots()
        
        # Exit the application
        print("🛑 CRITICAL ERROR - Application shutting down...")
        os._exit(1)  # Force exit
    
    def send_critical_alert(self, message):
        """Send critical alert immediately (bypass batching)"""
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            print(f"📱 Telegram not configured, but would send: {message}")
            return
            
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "Markdown"
            }
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                print("📱 CRITICAL ALERT SENT TO TELEGRAM")
            else:
                print(f"❌ Failed to send critical alert: {response.status_code}")
        except Exception as e:
            print(f"❌ Critical alert failed: {e}")
    
    def stop_all_bots(self):
        """Stop all running bots"""
        print("🛑 Stopping all bots...")
        for bot in self.bots:
            bot.running = False
        print("✅ All bots stopped")

# Initialize global error handler
error_handler = GlobalErrorHandler()

# Set global exception handler
sys.excepthook = error_handler.global_exception_handler

# ==================== TELEGRAM ALERTS ====================
def send_telegram(message, immediate=True):
    """Send Telegram message immediately"""
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
            print(f"📱 Telegram sent: {message[:50]}...")
        else:
            print(f"❌ Telegram error: {response.status_code}")
    except Exception as e:
        print(f"❌ Telegram failed: {e}")

# ==================== UTILITIES ====================
def get_ist_time():
    utc_now = datetime.now(timezone.utc)
    ist_offset = timedelta(hours=5, minutes=30)
    ist_time = utc_now + ist_offset
    return ist_time.strftime("%H:%M:%S")

def get_current_expiry():
    """Get current date in DDMMYY format"""
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    return ist_now.strftime("%d%m%y")

def format_expiry_display(expiry_code):
    """Convert DDMMYY to DD MMM YY format"""
    try:
        day = expiry_code[:2]
        month = expiry_code[2:4]
        year = "20" + expiry_code[4:6]
        
        month_names = {
            '01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr',
            '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug',
            '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'
        }
        
        return f"{day} {month_names[month]} {year}"
    except:
        return expiry_code

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

# ==================== EXPIRY MANAGEMENT ====================
class ExpiryManager:
    def __init__(self):
        self.current_expiry = get_current_expiry()
        self.active_expiry = self.get_initial_active_expiry()
        self.last_expiry_check = 0
        self.expiry_check_interval = 60  # Check every 60 seconds
    
    def get_initial_active_expiry(self):
        """Determine which expiry should be active right now"""
        now = datetime.now(timezone.utc)
        ist_now = now + timedelta(hours=5, minutes=30)
        
        if ist_now.hour >= 17 and ist_now.minute >= 30:
            next_day = ist_now + timedelta(days=1)
            next_expiry = next_day.strftime("%d%m%y")
            print(f"[{datetime.now()}] 🕠 After 5:30 PM, starting with next expiry: {next_expiry}")
            return next_expiry
        else:
            print(f"[{datetime.now()}] 📅 Starting with today's expiry: {self.current_expiry}")
            return self.current_expiry

    def should_rollover_expiry(self):
        """Check if we should move to next expiry"""
        now = datetime.now(timezone.utc)
        ist_now = now + timedelta(hours=5, minutes=30)
        
        if ist_now.hour >= 17 and ist_now.minute >= 30:
            next_expiry = (ist_now + timedelta(days=1)).strftime("%d%m%y")
            return next_expiry
        return None

    def get_available_expiries(self, asset):
        """Get all available expiries from Delta API"""
        try:
            url = "https://api.delta.exchange/v2/products"
            params = {
                'contract_types': 'call_options,put_options',
                'states': 'live'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                products = response.json().get('result', [])
                expiries = set()
                
                for product in products:
                    symbol = product.get('symbol', '')
                    if asset in symbol:
                        expiry = self.extract_expiry_from_symbol(symbol)
                        if expiry:
                            expiries.add(expiry)
                
                return sorted(expiries)
            return []
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Error fetching {asset} expiries: {e}")
            return []

    def extract_expiry_from_symbol(self, symbol):
        """Extract expiry date from symbol string"""
        try:
            parts = symbol.split('-')
            if len(parts) >= 4:
                return parts[3]
            return None
        except:
            return None

    def get_next_available_expiry(self, asset, current_expiry):
        """Get the next available expiry after current one"""
        available_expiries = self.get_available_expiries(asset)
        if not available_expiries:
            return current_expiry
        
        print(f"[{datetime.now()}] 📊 {asset}: Available expiries: {available_expiries}")
        
        for expiry in available_expiries:
            if expiry > current_expiry:
                return expiry
        
        return available_expiries[-1] if available_expiries else current_expiry

    def check_and_update_expiry(self, asset):
        """Check if we need to update the active expiry"""
        current_time = datetime.now().timestamp()
        if current_time - self.last_expiry_check >= self.expiry_check_interval:
            self.last_expiry_check = current_time
            
            current_time_str = get_ist_time()
            print(f"[{datetime.now()}] 🔄 {asset}: Checking expiry rollover... (Current: {self.active_expiry}, Time: {current_time_str})")
            
            next_expiry = self.should_rollover_expiry()
            if next_expiry and next_expiry != self.active_expiry:
                print(f"[{datetime.now()}] 🎯 {asset}: EXPIRY ROLLOVER TRIGGERED!")
                print(f"[{datetime.now()}] 📅 {asset}: Changing from {self.active_expiry} to {next_expiry}")
                
                actual_next_expiry = self.get_next_available_expiry(asset, self.active_expiry)
                
                if actual_next_expiry != self.active_expiry:
                    old_expiry = self.active_expiry
                    self.active_expiry = actual_next_expiry
                    
                    # Send Telegram notification
                    expiry_display = format_expiry_display(self.active_expiry)
                    send_telegram(f"🔄 {asset} Expiry Rollover Complete!\n\n📅 Now monitoring: {expiry_display}\n⏰ Time: {current_time_str}")
                    return True
                else:
                    print(f"[{datetime.now()}] ⚠️ {asset}: No new expiry available yet, keeping: {self.active_expiry}")
            
            # Check if current expiry is still available
            available_expiries = self.get_available_expiries(asset)
            if available_expiries and self.active_expiry not in available_expiries:
                print(f"[{datetime.now()}] ⚠️ {asset}: Current expiry {self.active_expiry} no longer available!")
                next_available = self.get_next_available_expiry(asset, self.active_expiry)
                if next_available != self.active_expiry:
                    print(f"[{datetime.now()}] 🔄 {asset}: Switching to available expiry: {next_available}")
                    self.active_expiry = next_available
                    
                    expiry_display = format_expiry_display(self.active_expiry)
                    send_telegram(f"🔄 {asset} Expiry Update!\n\n📅 Now monitoring: {expiry_display}\n⏰ Time: {current_time_str}")
                    return True
        
        return False

# ==================== LIVE MARKET DATA ====================
class LiveMarketData:
    def __init__(self):
        self.expiry_manager = ExpiryManager()
        self.eth_prices = {}
        self.btc_prices = {}
        self.last_data_fetch = 0
        self.data_fetch_interval = 2  # Fetch every 2 seconds
    
    def fetch_live_market_data(self, asset):
        """Fetch REAL trading data from Delta Exchange"""
        try:
            current_time = time.time()
            if current_time - self.last_data_fetch < self.data_fetch_interval:
                # Return cached data if too soon
                return self.eth_prices if asset == "ETH" else self.btc_prices
            
            self.last_data_fetch = current_time
            
            # First, check and update expiry
            self.expiry_manager.check_and_update_expiry(asset)
            current_expiry = self.expiry_manager.active_expiry
            
            # Fetch all products
            url = "https://api.delta.exchange/v2/products"
            params = {
                'contract_types': 'call_options,put_options',
                'states': 'live'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                products = response.json().get('result', [])
                market_data = {}
                
                for product in products:
                    symbol = product.get('symbol', '')
                    
                    # Filter for current asset and expiry
                    if asset in symbol and current_expiry in symbol:
                        product_id = product.get('id')
                        
                        # Fetch ticker data for current prices
                        ticker_url = f"https://api.delta.exchange/v2/tickers"
                        ticker_params = {'symbol': symbol}
                        
                        ticker_response = requests.get(ticker_url, params=ticker_params, timeout=10)
                        
                        if ticker_response.status_code == 200:
                            ticker_data = ticker_response.json()
                            if ticker_data.get('result'):
                                ticker = ticker_data['result'][0]
                                quotes = ticker.get('quotes', {})
                                
                                bid_price = float(quotes.get('best_bid', 0))
                                ask_price = float(quotes.get('best_ask', 0))
                                
                                # Only include options with valid prices
                                if bid_price > 0 and ask_price > 0 and bid_price != ask_price:
                                    market_data[symbol] = {
                                        'symbol': symbol,
                                        'bid': bid_price,
                                        'ask': ask_price,
                                        'qty': 50  # Default quantity for paper trading
                                    }
                
                # Update cache
                if asset == "ETH":
                    self.eth_prices = market_data
                else:
                    self.btc_prices = market_data
                
                print(f"✅ {asset}: Fetched {len(market_data)} live options for expiry {current_expiry}")
                return market_data
            else:
                print(f"❌ {asset}: API Error {response.status_code}")
                return {}
                
        except Exception as e:
            print(f"❌ {asset}: Error fetching live data: {e}")
            return self.eth_prices if asset == "ETH" else self.btc_prices  # Return cached data on error

# ==================== ARBITRAGE ENGINE ====================
class UltraFastArbitrageEngine:
    def __init__(self):
        self.market_data = LiveMarketData()
    
    def fetch_data(self, asset):
        """Fetch live market data"""
        return self.market_data.fetch_live_market_data(asset)
    
    def find_arbitrage_opportunities(self, asset, options_data):
        """Ultra-fast arbitrage detection with REAL data"""
        if not options_data:
            return []
            
        opportunities = []
        strikes = self.group_options_by_strike(options_data)
        sorted_strikes = sorted(strikes.keys())
        
        if len(sorted_strikes) < 2:
            return []
        
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
                print(f"🎯 {asset} CALL Opportunity: {strike1}→{strike2} | Profit: ${profit:.2f}")
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
                print(f"🎯 {asset} PUT Opportunity: {strike1}→{strike2} | Profit: ${profit:.2f}")
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
                send_telegram(f"✅ {asset} SELL FILLED: {filled_qty} lots @ ${price:.2f}")
                print(f"📝 PAPER: SELL {filled_qty}/{quantity} {symbol} @ ${price:.2f} - FULL FILL")
            else:
                # Partial fill
                timeline.add_step(f"SELL: {filled_qty}/{quantity} lots @ ${price:.2f}", "✅")
                timeline.add_step(f"SELL: {quantity-filled_qty} lots CANCELLED", "❌")
                send_telegram(f"⚠️ {asset} SELL PARTIAL FILL: {filled_qty}/{quantity} lots @ ${price:.2f}")
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
        send_telegram(f"📝 {asset} BUY ATTEMPT: {quantity} lots @ ${current_price:.2f}")
        
        if PAPER_TRADING:
            # Realistic simulation
            scenario = random.choices(
                ['instant', 'adjustment', 'match_price', 'abandon'],
                weights=[0.4, 0.3, 0.2, 0.1]
            )[0]
            
            if scenario == 'instant':
                time.sleep(0.1)
                timeline.add_step(f"BUY: {quantity} lots @ ${current_price:.2f}", "✅")
                send_telegram(f"✅ {asset} BUY FILLED: {quantity} lots @ ${current_price:.2f}")
                return True, current_price, timeline
            
            elif scenario == 'adjustment':
                time.sleep(0.2)
                current_price += asset_params['price_increment']
                timeline.add_step(f"BUY not filled → ${current_price:.2f}", "🔄")
                send_telegram(f"🔄 {asset} BUY ADJUSTMENT: ${current_price:.2f}")
                time.sleep(0.1)
                timeline.add_step(f"BUY: {quantity} lots @ ${current_price:.2f}", "✅")
                send_telegram(f"✅ {asset} BUY FILLED: {quantity} lots @ ${current_price:.2f}")
                return True, current_price, timeline
            
            elif scenario == 'match_price':
                time.sleep(0.2)
                current_price += asset_params['price_increment']
                timeline.add_step(f"BUY not filled → ${current_price:.2f}", "🔄")
                send_telegram(f"🔄 {asset} BUY ADJUSTMENT: ${current_price:.2f}")
                time.sleep(0.2)
                current_price = sell_price
                timeline.add_step(f"BUY not filled → ${current_price:.2f} (sell price)", "🚀")
                send_telegram(f"🚀 {asset} BUY MATCH SELL: ${current_price:.2f}")
                time.sleep(0.1)
                timeline.add_step(f"BUY: {quantity} lots @ ${current_price:.2f}", "✅")
                send_telegram(f"✅ {asset} BUY FILLED: {quantity} lots @ ${current_price:.2f}")
                return True, current_price, timeline
            
            else:  # abandon
                time.sleep(0.2)
                current_price += asset_params['price_increment']
                timeline.add_step(f"BUY not filled → ${current_price:.2f}", "🔄")
                send_telegram(f"🔄 {asset} BUY ADJUSTMENT: ${current_price:.2f}")
                time.sleep(0.2)
                current_price = sell_price
                timeline.add_step(f"BUY not filled → ${current_price:.2f} (sell price)", "🚀")
                send_telegram(f"🚀 {asset} BUY MATCH SELL: ${current_price:.2f}")
                time.sleep(0.1)
                timeline.add_step("BUY ABANDONED - Not filled", "❌")
                send_telegram(f"❌ {asset} BUY ABANDONED: Manual intervention needed")
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
            
            # Send trade attempt alert
            send_telegram(f"🎯 {asset} TRADE ATTEMPT: {opportunity['type']} {opportunity['strike1']}→{opportunity['strike2']} | Qty: {trade_qty}")
            
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
        
        # Register with global error handler
        error_handler.register_bot(self)
    
    def ultra_fast_monitoring(self):
        """Ultra-fast monitoring loop with comprehensive error handling"""
        print(f"🚀 Starting Ultra-Fast {self.asset} Bot with LIVE Data")
        
        while self.running:
            cycle_start = time.time()
            self.cycle_count += 1
            
            try:
                # 1. Fetch LIVE market data
                data = self.arbitrage_engine.fetch_data(self.asset)
                
                # 2. Find opportunities with REAL data
                opportunities = self.arbitrage_engine.find_arbitrage_opportunities(self.asset, data)
                
                # 3. Execute immediately if opportunities found
                if opportunities:
                    print(f"🎯 {self.asset}: Found {len(opportunities)} opportunities")
                    self.order_executor.execute_arbitrage_trade(self.asset, opportunities[0])
                
                # 4. No status updates - only trade alerts
                
                # 5. No sleep - immediate next cycle
                elapsed_cycle = time.time() - cycle_start
                if elapsed_cycle < 0.1:  # If cycle too fast
                    time.sleep(0.05)  # 50ms tiny pause
                    
            except Exception as e:
                print(f"❌ {self.asset} Bot error: {e}")
                send_telegram(f"❌ {self.asset} Bot error: {str(e)}")
                
                # If it's a critical error, trigger global shutdown
                if self.is_critical_error(e):
                    raise e  # This will trigger the global exception handler
                else:
                    time.sleep(0.5)  # Brief pause on non-critical error
    
    def is_critical_error(self, error):
        """Determine if an error is critical enough to stop everything"""
        critical_errors = [
            "ConnectionError",
            "Timeout", 
            "APIError",
            "KeyboardInterrupt",
            "SystemExit",
            "MemoryError",
            "RuntimeError"
        ]
        
        error_type = type(error).__name__
        error_str = str(error).lower()
        
        # Network/connection issues
        if any(err in error_type for err in ['Connection', 'Timeout', 'API']):
            return True
        
        # Python environment issues
        if any(err in error_type for err in ['Memory', 'System', 'Keyboard']):
            return True
        
        # Specific error messages indicating critical failure
        critical_phrases = [
            'max retries exceeded',
            'connection broken',
            'invalid api key',
            'rate limit exceeded',
            'out of memory',
            'no module named'
        ]
        
        if any(phrase in error_str for phrase in critical_phrases):
            return True
        
        return False

# ==================== FLASK ROUTES ====================
@app.route('/')
def home():
    return f"""
    <h1>🚀 Ultra-Fast Crypto Arbitrage Bot</h1>
    <p><strong>Status:</strong> Running - LIVE Market Data</p>
    <p><strong>Paper Trading:</strong> {PAPER_TRADING}</p>
    <p><strong>ETH:</strong> ${ETH_PARAMS['min_profit']} min profit</p>
    <p><strong>BTC:</strong> ${BTC_PARAMS['min_profit']} min profit</p>
    <p><strong>Data Source:</strong> Delta Exchange LIVE API</p>
    <p><strong>Features:</strong> Live Data ✅ | Partial Fills ✅ | Auto Expiry ✅ | Trade Alerts Only ✅ | Critical Error Protection ✅</p>
    <p><a href="/health">Health Check</a></p>
    """

@app.route('/health')
def health():
    return {
        "status": "healthy",
        "mode": "ultra_fast_live_data",
        "paper_trading": PAPER_TRADING,
        "eth_min_profit": ETH_PARAMS['min_profit'],
        "btc_min_profit": BTC_PARAMS['min_profit'],
        "data_source": "delta_exchange_live_api",
        "features": ["live_market_data", "partial_fill_handling", "auto_expiry_rollover", "trade_alerts_only", "critical_error_protection"],
        "timestamp": get_ist_time()
    }

# ==================== INITIALIZATION ====================
eth_bot = UltraFastAPIBot("ETH")
btc_bot = UltraFastAPIBot("BTC")

def start_ultra_fast_bots():
    """Start both bots with LIVE market data"""
    print("🚀 Starting Ultra-Fast Crypto Arbitrage Bot with LIVE Data...")
    print(f"🔵 ETH: ${ETH_PARAMS['min_profit']} min profit, ${ETH_PARAMS['price_increment']} increments")
    print(f"🟡 BTC: ${BTC_PARAMS['min_profit']} min profit, ${BTC_PARAMS['price_increment']} increments")
    print(f"⚡ Polling: Maximum Speed with LIVE Delta Exchange Data")
    print(f"🔄 Features: Live Data + Partial Fills + Auto Expiry Rollover + Trade Alerts Only + Critical Error Protection")
    print(f"📝 Paper Trading: {PAPER_TRADING}")
    
    # Start bots in separate threads
    eth_thread = threading.Thread(target=eth_bot.ultra_fast_monitoring, daemon=True)
    btc_thread = threading.Thread(target=btc_bot.ultra_fast_monitoring, daemon=True)
    
    eth_thread.start()
    btc_thread.start()
    
    # Send startup message
    send_telegram(f"🤖 Ultra-Fast Arbitrage Bot Started\n\n🔵 ETH: ${ETH_PARAMS['min_profit']} min profit\n🟡 BTC: ${BTC_PARAMS['min_profit']} min profit\n⚡ Polling: Maximum Speed\n📊 Data: LIVE Delta Exchange\n🔄 Partial Fills: Enabled\n📅 Auto Expiry: Enabled\n📝 Paper Trading: {PAPER_TRADING}\n🔔 Alerts: Trade Events Only\n🛑 Critical Error Protection: ENABLED")

if __name__ == "__main__":
    start_ultra_fast_bots()
    
    port = int(os.environ.get("PORT", 10000))
    print(f"🌐 Starting Flask server on port {port}")
    
    try:
        app.run(
            host='0.0.0.0',
            port=port,
            debug=False,
            threaded=True
        )
    except Exception as e:
        # If Flask server fails, trigger global shutdown
        error_handler.global_exception_handler(type(e), e, e.__traceback__)
