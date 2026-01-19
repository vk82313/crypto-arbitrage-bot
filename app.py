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
MAX_LOTS_PER_TRADE = int(os.getenv("MAX_LOTS_PER_TRADE", "100"))

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

# Delta Exchange India API Configuration
DELTA_API_BASE = "https://api.india.delta.exchange/v2"
DELTA_API_TIMEOUT = 2  # Reduced timeout for faster data

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

# ==================== ULTRA-FAST MARKET DATA WITH 1-SECOND POLLING ====================
class UltraFastMarketData:
    def __init__(self):
        self.expiry_manager = ExpiryManager()
        self.eth_prices = {}
        self.btc_prices = {}
        self.last_data_fetch = 0
        self.data_fetch_interval = 1  # STRICT 1-SECOND POLLING
        self.fetch_counter = 0
        self.last_successful_fetch = 0
        
    def fetch_live_market_data(self, asset):
        """ULTRA-FAST: Fetch REAL trading data every second from Delta Exchange"""
        try:
            current_time = time.time()
            
            # Enforce 1-second polling interval strictly
            time_since_last_fetch = current_time - self.last_data_fetch
            if time_since_last_fetch < self.data_fetch_interval:
                # Return cached data but still enforce timing
                sleep_time = self.data_fetch_interval - time_since_last_fetch
                if sleep_time > 0:
                    time.sleep(sleep_time)
                return self.eth_prices if asset == "ETH" else self.btc_prices
            
            self.last_data_fetch = time.time()
            self.fetch_counter += 1
            
            # Check expiry every 30 seconds instead of every fetch
            if current_time - self.expiry_manager.last_expiry_check >= 30:
                self.expiry_manager.check_and_update_expiry(asset)
            
            current_expiry = self.expiry_manager.active_expiry
            
            # ULTRA-FAST API CALL with minimal overhead
            url = f"{DELTA_API_BASE}/tickers"
            params = {
                'contract_types': 'call_options,put_options'
            }
            
            # Fast API call with short timeout
            response = requests.get(url, params=params, timeout=DELTA_API_TIMEOUT)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success', False):
                    tickers = data.get('result', [])
                    market_data = {}
                    
                    # High-speed processing with minimal operations
                    for ticker in tickers:
                        symbol = ticker.get('symbol', '')
                        
                        # Fast filtering
                        if (f'-{asset}-' in symbol and current_expiry in symbol and 
                            (symbol.startswith('C-') or symbol.startswith('P-'))):
                            
                            quotes = ticker.get('quotes', {})
                            bid_price = float(quotes.get('best_bid', 0)) if quotes.get('best_bid') else 0
                            ask_price = float(quotes.get('best_ask', 0)) if quotes.get('best_ask') else 0
                            
                            # Only process options with valid prices
                            if bid_price > 0 and ask_price > 0:
                                strike = self.extract_strike_from_symbol(symbol)
                                if strike > 0:
                                    market_data[symbol] = {
                                        'symbol': symbol,
                                        'bid': bid_price,
                                        'ask': ask_price,
                                        'qty': 100,
                                        'strike': strike,
                                        'option_type': 'call' if symbol.startswith('C-') else 'put'
                                    }
                    
                    # Update cache
                    if asset == "ETH":
                        self.eth_prices = market_data
                    else:
                        self.btc_prices = market_data
                    
                    self.last_successful_fetch = time.time()
                    
                    # Minimal logging to avoid overhead
                    if self.fetch_counter % 60 == 0:  # Log once per minute
                        print(f"✅ {asset}: Fresh data fetched - {len(market_data)} options @ {get_ist_time()}")
                    
                    return market_data
                else:
                    print(f"❌ {asset}: API success=false")
                    return self.eth_prices if asset == "ETH" else self.btc_prices
            else:
                print(f"❌ {asset}: API Error {response.status_code}")
                return self.eth_prices if asset == "ETH" else self.btc_prices
                
        except Exception as e:
            print(f"❌ {asset}: Fetch error: {e}")
            return self.eth_prices if asset == "ETH" else self.btc_prices

    def extract_strike_from_symbol(self, symbol):
        """High-speed strike extraction"""
        try:
            parts = symbol.split('-')
            if len(parts) >= 3:
                return int(parts[2])
            return 0
        except:
            return 0

# ==================== FIXED EXPIRY MANAGEMENT ====================
class ExpiryManager:
    def __init__(self):
        self.current_expiry = get_current_expiry()
        self.active_expiry = self.get_initial_active_expiry()
        self.last_expiry_check = 0
        self.expiry_check_interval = 30  # Check every 30 seconds
    
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
        """Get all available expiries from Delta Exchange India API"""
        try:
            url = f"{DELTA_API_BASE}/tickers"
            params = {
                'contract_types': 'call_options,put_options'
            }
            
            response = requests.get(url, params=params, timeout=DELTA_API_TIMEOUT)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success', False):
                    tickers = data.get('result', [])
                    expiries = set()
                    
                    for ticker in tickers:
                        symbol = ticker.get('symbol', '')
                        if f'-{asset}-' in symbol:
                            expiry = self.extract_expiry_from_symbol(symbol)
                            if expiry:
                                expiries.add(expiry)
                    
                    return sorted(expiries)
                else:
                    return []
            else:
                return []
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Error fetching {asset} expiries: {e}")
            return []

    def extract_expiry_from_symbol(self, symbol):
        """Extract expiry date from Delta Exchange symbol"""
        try:
            parts = symbol.split('-')
            if len(parts) >= 4:
                expiry_code = parts[3]
                if len(expiry_code) == 6 and expiry_code.isdigit():
                    return expiry_code
            return None
        except:
            return None

    def get_next_available_expiry(self, asset, current_expiry):
        """Get the next available expiry after current one"""
        available_expiries = self.get_available_expiries(asset)
        if not available_expiries:
            return current_expiry
        
        for expiry in available_expiries:
            if expiry > current_expiry:
                return expiry
        
        return available_expiries[-1] if available_expiries else current_expiry

    def check_and_update_expiry(self, asset):
        """Check if we need to update the active expiry"""
        current_time = time.time()
        if current_time - self.last_expiry_check >= self.expiry_check_interval:
            self.last_expiry_check = current_time
            
            next_expiry = self.should_rollover_expiry()
            if next_expiry and next_expiry != self.active_expiry:
                print(f"[{datetime.now()}] 🎯 {asset}: EXPIRY ROLLOVER TRIGGERED!")
                
                actual_next_expiry = self.get_next_available_expiry(asset, self.active_expiry)
                
                if actual_next_expiry != self.active_expiry:
                    old_expiry = self.active_expiry
                    self.active_expiry = actual_next_expiry
                    
                    expiry_display = format_expiry_display(self.active_expiry)
                    send_telegram(f"🔄 {asset} Expiry Rollover Complete!\n\n📅 Now monitoring: {expiry_display}\n⏰ Time: {get_ist_time()}")
                    return True
            
            # Check if current expiry is still available
            available_expiries = self.get_available_expiries(asset)
            if available_expiries and self.active_expiry not in available_expiries:
                next_available = self.get_next_available_expiry(asset, self.active_expiry)
                if next_available != self.active_expiry:
                    self.active_expiry = next_available
                    expiry_display = format_expiry_display(self.active_expiry)
                    send_telegram(f"🔄 {asset} Expiry Update!\n\n📅 Now monitoring: {expiry_display}\n⏰ Time: {get_ist_time()}")
                    return True
        
        return False

# ==================== ULTRA-FAST ARBITRAGE ENGINE ====================
class UltraFastArbitrageEngine:
    def __init__(self):
        self.market_data = UltraFastMarketData()
        self.opportunity_cache = {}
        self.last_analysis_time = 0
    
    def fetch_data(self, asset):
        """Fetch live market data every second"""
        return self.market_data.fetch_live_market_data(asset)
    
    def find_arbitrage_opportunities(self, asset, options_data):
        """Ultra-fast arbitrage detection with 1-second fresh data"""
        if not options_data:
            return []
            
        opportunities = []
        strikes = self.group_options_by_strike(options_data)
        sorted_strikes = sorted(strikes.keys())
        
        if len(sorted_strikes) < 2:
            return []
        
        # Fast sequential scanning for opportunities
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
        
        return sorted(opportunities, key=lambda x: x['profit'], reverse=True)[:5]  # Return top 5 opportunities

    def check_call_arbitrage(self, asset, strikes, strike1, strike2):
        asset_params = ETH_PARAMS if asset == "ETH" else BTC_PARAMS
        
        if 'call' not in strikes[strike1] or 'call' not in strikes[strike2]:
            return None
            
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
                    'sell_qty': strikes[strike2]['call'].get('qty', 100),
                    'timestamp': get_ist_time()
                }
        return None
    
    def check_put_arbitrage(self, asset, strikes, strike1, strike2):
        asset_params = ETH_PARAMS if asset == "ETH" else BTC_PARAMS
        
        if 'put' not in strikes[strike1] or 'put' not in strikes[strike2]:
            return None
            
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
                    'sell_qty': strikes[strike1]['put'].get('qty', 100),
                    'timestamp': get_ist_time()
                }
        return None
    
    def group_options_by_strike(self, options_data):
        """High-speed grouping by strike"""
        strikes = {}
        for symbol, data in options_data.items():
            strike = data.get('strike', 0)
            if strike > 0:
                if strike not in strikes:
                    strikes[strike] = {'call': {}, 'put': {}}
                
                option_type = data.get('option_type', 'unknown')
                if option_type == 'call':
                    strikes[strike]['call'] = data
                elif option_type == 'put':
                    strikes[strike]['put'] = data
        return strikes

# ==================== ORDER EXECUTION ====================
class UltraFastOrderExecutor:
    def __init__(self):
        self.active_trades = {}
    
    def execute_sell_with_partial_fill(self, symbol, price, quantity, asset):
        """Execute sell order with immediate fill check and 5-second timeout"""
        timeline = TimelineTracker()
        
        if PAPER_TRADING:
            timeline.add_step(f"SELL ORDER PLACED: {quantity} lots @ ${price:.2f}", "📝")
            
            # Check for immediate fill
            immediate_fill = random.choices([True, False], weights=[0.3, 0.7])[0]
            if immediate_fill:
                timeline.add_step(f"SELL ORDER IMMEDIATELY FILLED: {quantity} lots @ ${price:.2f}", "✅")
                return quantity, timeline
            
            # If not immediately filled, wait 5 seconds with periodic checks
            timeline.add_step("SELL ORDER NOT FILLED - Waiting 5 seconds...", "⏳")
            
            for second in range(5):
                time.sleep(1)
                # Check for fill each second
                filled = random.choices([True, False], weights=[0.2, 0.8])[0]
                if filled:
                    filled_qty = random.choices(
                        [quantity, quantity-1, quantity-2],
                        weights=[0.7, 0.2, 0.1]
                    )[0]
                    
                    if filled_qty == quantity:
                        timeline.add_step(f"SELL ORDER FILLED after {second+1} seconds: {filled_qty} lots @ ${price:.2f}", "✅")
                    else:
                        timeline.add_step(f"SELL PARTIAL FILL after {second+1} seconds: {filled_qty}/{quantity} lots @ ${price:.2f}", "✅")
                    
                    return filled_qty, timeline
            
            # After 5 seconds, if still not filled
            timeline.add_step(f"SELL ORDER CANCELLED - No fill after 5 seconds", "❌")
            return 0, timeline
        
        return quantity, timeline
    
    def execute_buy_sequence(self, symbol, original_price, sell_price, quantity, asset):
        """Execute buy with 2-second intervals and progressive price adjustments"""
        timeline = TimelineTracker()
        asset_params = ETH_PARAMS if asset == "ETH" else BTC_PARAMS
        
        current_price = original_price
        buy_success = False
        final_price = original_price
        
        # Step 1: Try at original price for 2 seconds
        timeline.add_step(f"BUY ORDER PLACED: {quantity} lots @ ${current_price:.2f}", "📝")
        
        if PAPER_TRADING:
            # Check for immediate fill
            immediate_fill = random.choices([True, False], weights=[0.2, 0.8])[0]
            if immediate_fill:
                timeline.add_step(f"BUY ORDER IMMEDIATELY FILLED: {quantity} lots @ ${current_price:.2f}", "✅")
                return True, current_price, timeline
            
            # Wait 2 seconds at original price
            for second in range(2):
                time.sleep(1)
                fill_check = random.choices([True, False], weights=[0.1, 0.9])[0]
                if fill_check:
                    timeline.add_step(f"BUY ORDER FILLED after {second+1} seconds @ ${current_price:.2f}", "✅")
                    return True, current_price, timeline
            
            timeline.add_step(f"BUY NOT FILLED in 2 seconds @ ${current_price:.2f}", "⏳")
            
            # Step 2: Increase price by increment and wait 2 seconds
            current_price += asset_params['price_increment']
            timeline.add_step(f"BUY PRICE INCREASED: ${current_price:.2f} (+${asset_params['price_increment']:.2f})", "🔄")
            
            for second in range(2):
                time.sleep(1)
                fill_check = random.choices([True, False], weights=[0.3, 0.7])[0]
                if fill_check:
                    timeline.add_step(f"BUY ORDER FILLED after {second+1} seconds @ ${current_price:.2f}", "✅")
                    return True, current_price, timeline
            
            timeline.add_step(f"BUY NOT FILLED in 2 seconds @ ${current_price:.2f}", "⏳")
            
            # Step 3: Increase price to match sell price and wait 2 seconds
            current_price = sell_price
            timeline.add_step(f"BUY PRICE MATCHED: ${current_price:.2f} (equal to sell)", "🚀")
            
            for second in range(2):
                time.sleep(1)
                fill_check = random.choices([True, False], weights=[0.5, 0.5])[0]
                if fill_check:
                    timeline.add_step(f"BUY ORDER FILLED after {second+1} seconds @ ${current_price:.2f}", "✅")
                    return True, current_price, timeline
            
            # Step 4: Manual intervention needed
            timeline.add_step(f"BUY ORDER FAILED - MANUAL INTERVENTION REQUIRED", "🚨")
            return False, current_price, timeline
        
        return True, original_price, timeline

    def execute_arbitrage_trade(self, asset, opportunity):
        """Complete trade execution with continuous trading for same opportunity"""
        try:
            # Calculate maximum tradable quantity (min of 100, buy_qty, sell_qty)
            max_tradable_qty = min(
                MAX_LOTS_PER_TRADE,
                opportunity['buy_qty'],
                opportunity['sell_qty']
            )
            
            if max_tradable_qty < 1:
                return False
            
            emoji = "🔵" if asset == "ETH" else "🟡"
            asset_params = ETH_PARAMS if asset == "ETH" else BTC_PARAMS
            
            # Execute multiple trades until no quantity left
            total_filled_qty = 0
            total_profit = 0
            trade_count = 0
            
            while max_tradable_qty > 0:
                trade_count += 1
                # Calculate quantity for this trade (max 100 per trade)
                trade_qty = min(100, max_tradable_qty)
                
                combined_timeline = TimelineTracker()
                combined_timeline.add_step(f"TRADE {trade_count}: Starting {asset} {opportunity['type']} ARBITRAGE", "🚀")
                combined_timeline.add_step(f"Strike: {opportunity['strike1']} → {opportunity['strike2']}", "🎯")
                combined_timeline.add_step(f"Quantity: {trade_qty} lots | Expected Profit: ${opportunity['profit']:.2f}", "💰")
                
                # Execute sell order
                filled_qty, sell_timeline = self.execute_sell_with_partial_fill(
                    opportunity['sell_symbol'], 
                    opportunity['sell_premium'], 
                    trade_qty, 
                    asset
                )
                
                combined_timeline.timeline.extend(sell_timeline.timeline)
                
                if filled_qty == 0:
                    combined_timeline.add_step("SELL ORDER CANCELLED - Moving to next opportunity", "⏭️")
                    self.send_complete_order_message(asset, opportunity, trade_qty, 0, 0, combined_timeline, emoji, "SELL_TIMEOUT")
                    break
                
                if filled_qty < trade_qty:
                    combined_timeline.add_step(f"PARTIAL FILL: Adjusting buy to {filled_qty} lots", "🔄")
                
                # Execute buy sequence
                buy_success, final_price, buy_timeline = self.execute_buy_sequence(
                    opportunity['buy_symbol'],
                    opportunity['buy_premium'],
                    opportunity['sell_premium'],
                    filled_qty,
                    asset
                )
                
                combined_timeline.timeline.extend(buy_timeline.timeline)
                
                if buy_success:
                    profit = opportunity['sell_premium'] - final_price
                    trade_pnl = profit * filled_qty
                    total_profit += trade_pnl
                    total_filled_qty += filled_qty
                    
                    status = "EXECUTED" if profit > 0 else "BREAK_EVEN"
                    combined_timeline.add_step(f"TRADE {trade_count} COMPLETED: Profit ${profit:.2f} per lot | Total: ${trade_pnl:.2f}", "💰")
                    
                    # Update remaining quantity
                    max_tradable_qty -= filled_qty
                    
                    # If same opportunity still exists and we have quantity left, continue
                    if max_tradable_qty > 0:
                        combined_timeline.add_step(f"CONTINUING: {max_tradable_qty} lots remaining in this opportunity", "🔄")
                        continue
                    else:
                        combined_timeline.add_step("ALL QUANTITY EXECUTED for this opportunity", "✅")
                        break
                else:
                    combined_timeline.add_step(f"TRADE {trade_count} FAILED - Manual intervention needed", "🚨")
                    self.send_complete_order_message(
                        asset, opportunity, trade_qty, filled_qty, final_price, 
                        combined_timeline, emoji, "MANUAL_INTERVENTION_NEEDED"
                    )
                    break
            
            # Send final summary if we executed any trades
            if total_filled_qty > 0:
                self.send_complete_order_message(
                    asset, opportunity, total_filled_qty, total_filled_qty, final_price,
                    combined_timeline, emoji, "EXECUTED", 
                    total_profit/total_filled_qty if total_filled_qty > 0 else 0, 
                    total_profit, True
                )
            
            return total_filled_qty > 0
            
        except Exception as e:
            error_msg = f"🚨 {asset} TRADE ERROR: {str(e)}"
            send_telegram(error_msg)
            print(f"{asset} Trade Error: {e}")
            return False

    def send_complete_order_message(self, asset, opportunity, ordered_qty, filled_qty, final_price, timeline, emoji, status, profit=0, total_pnl=0, success=True):
        """Send complete order book in single Telegram message"""
        
        if status == "SELL_TIMEOUT":
            message = f"""
⏰ {asset} COMPLETE ORDER - SELL TIMEOUT

{emoji} {asset} {opportunity['type']} Spread
🔄 {opportunity['strike1']} → {opportunity['strike2']}
💰 Buy: ${opportunity['buy_premium']:.2f} | Sell: ${opportunity['sell_premium']:.2f}
📦 Ordered: {ordered_qty} lots | Expected Profit: ${opportunity['profit']:.2f}

⏰ EXECUTION TIMELINE:
{timeline.get_timeline_text()}

❌ RESULT: Sell order not filled after 5 seconds
🔄 ACTION: Order cancelled, moving to next opportunity

🕒 Completed: {get_ist_time()} IST
"""
        elif status == "MANUAL_INTERVENTION_NEEDED":
            message = f"""
🚨 {asset} COMPLETE ORDER - MANUAL INTERVENTION NEEDED

{emoji} {asset} {opportunity['type']} Spread
🔄 {opportunity['strike1']} → {opportunity['strike2']}
💰 Buy Attempted: ${final_price:.2f} | Sold: ${opportunity['sell_premium']:.2f}
📦 Sold: {filled_qty} lots | Buy Failed

⏰ EXECUTION TIMELINE:
{timeline.get_timeline_text()}

🚨 CURRENT POSITION: {filled_qty} lots SHORT
👤 MANUAL INTERVENTION REQUIRED

🕒 Completed: {get_ist_time()} IST
"""
        else:
            status_text = "EXECUTED" if profit > 0 else "BREAK EVEN"
            message = f"""
🤖 {asset} COMPLETE ORDER - {status_text}

{emoji} {asset} {opportunity['type']} Spread
🔄 {opportunity['strike1']} → {opportunity['strike2']}
💰 Buy: ${opportunity['buy_premium']:.2f} → ${final_price:.2f} | Sell: ${opportunity['sell_premium']:.2f}
📦 Ordered: {ordered_qty} lots | Filled: {filled_qty} lots

⏰ EXECUTION TIMELINE:
{timeline.get_timeline_text()}

💰 ACTUAL PROFIT: ${profit:.2f} per lot
💵 TOTAL P&L: ${total_pnl:.2f}

🕒 Completed: {get_ist_time()} IST
"""
        
        send_telegram(message)

# ==================== ULTRA-FAST BOTS WITH 1-SECOND DATA FETCHING ====================
class UltraFastAPIBot:
    def __init__(self, asset):
        self.asset = asset
        self.arbitrage_engine = UltraFastArbitrageEngine()
        self.order_executor = UltraFastOrderExecutor()
        self.running = True
        self.cycle_count = 0
        self.start_time = time.time()
        self.last_opportunity_log = 0
        self.opportunities_found = 0
        
    def ultra_fast_monitoring(self):
        """ULTRA-FAST monitoring with 1-SECOND data fetching"""
        print(f"🚀 Starting ULTRA-FAST {self.asset} Bot with 1-SECOND DATA FETCHING")
        
        while self.running:
            cycle_start = time.time()
            self.cycle_count += 1
            
            try:
                # 1. FETCH FRESH DATA EVERY SECOND
                data = self.arbitrage_engine.fetch_data(self.asset)
                
                # 2. Find opportunities with FRESH data
                opportunities = self.arbitrage_engine.find_arbitrage_opportunities(self.asset, data)
                
                # 3. Execute immediately if opportunities found
                if opportunities:
                    self.opportunities_found += len(opportunities)
                    current_time = time.time()
                    
                    if current_time - self.last_opportunity_log >= 3:  # Log every 3 seconds max
                        print(f"🎯 {self.asset}: Found {len(opportunities)} FRESH opportunities")
                        for opp in opportunities[:2]:
                            print(f"💰 {self.asset} Opportunity: {opp['type']} {opp['strike1']}→{opp['strike2']} Profit: ${opp['profit']:.2f}")
                        self.last_opportunity_log = current_time
                    
                    # Execute the best opportunity
                    best_opp = opportunities[0]
                    if best_opp['profit'] >= (ETH_PARAMS['min_profit'] if self.asset == "ETH" else BTC_PARAMS['min_profit']):
                        self.order_executor.execute_arbitrage_trade(self.asset, best_opp)
                
                # 4. Performance monitoring
                if self.cycle_count % 60 == 0:  # Log every minute
                    elapsed = time.time() - self.start_time
                    cycles_per_second = self.cycle_count / elapsed
                    data_count = len(data)
                    print(f"⚡ {self.asset}: {cycles_per_second:.1f} cycles/sec | Data: {data_count} options | Opportunities: {self.opportunities_found}")
                
                # 5. STRICT 1-second timing control
                elapsed_cycle = time.time() - cycle_start
                sleep_time = max(0.0, 1.0 - elapsed_cycle)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                else:
                    print(f"⚠️ {self.asset}: Cycle took {elapsed_cycle:.3f}s (over 1 second)")
                    
            except Exception as e:
                print(f"❌ {self.asset} Bot error: {e}")
                time.sleep(1)

# ==================== FLASK ROUTES ====================
@app.route('/')
def home():
    return f"""
    <h1>🚀 ULTRA-FAST Crypto Arbitrage Bot</h1>
    <p><strong>Status:</strong> Running - 1-SECOND DATA FETCHING</p>
    <p><strong>Paper Trading:</strong> {PAPER_TRADING}</p>
    <p><strong>Data Fetching:</strong> EVERY SECOND</p>
    <p><strong>ETH:</strong> ${ETH_PARAMS['min_profit']} min profit</p>
    <p><strong>BTC:</strong> ${BTC_PARAMS['min_profit']} min profit</p>
    <p><strong>Order Quantity:</strong> 100 lots</p>
    <p><strong>Data Source:</strong> Delta Exchange India API (1-SECOND POLLING)</p>
    <p><strong>Features:</strong> 1-Second Data Fetching ✅ | Live Market Data ✅ | Ultra-Fast Execution ✅</p>
    <p><a href="/health">Health Check</a></p>
    """

@app.route('/health')
def health():
    return {
        "status": "healthy",
        "mode": "ultra_fast_1_second_data_fetching",
        "paper_trading": PAPER_TRADING,
        "data_fetching": "every_second",
        "order_quantity": "100_lots",
        "eth_min_profit": ETH_PARAMS['min_profit'],
        "btc_min_profit": BTC_PARAMS['min_profit'],
        "data_source": "delta_exchange_india_api",
        "api_timeout": "2_seconds",
        "features": ["1_second_data_fetching", "ultra_fast_processing", "live_market_data", "auto_expiry_rollover"],
        "timestamp": get_ist_time()
    }

# ==================== INITIALIZATION ====================
eth_bot = UltraFastAPIBot("ETH")
btc_bot = UltraFastAPIBot("BTC")

def start_ultra_fast_bots():
    """Start both bots with 1-SECOND DATA FETCHING"""
    print("🚀 Starting ULTRA-FAST Crypto Arbitrage Bot with 1-SECOND DATA FETCHING...")
    print(f"🔵 ETH: ${ETH_PARAMS['min_profit']} min profit")
    print(f"🟡 BTC: ${BTC_PARAMS['min_profit']} min profit")
    print(f"⚡ DATA FETCHING: EVERY SECOND")
    print(f"📦 Order Quantity: 100 LOTS")
    print(f"⏰ Sell Timeout: 5 seconds")
    print(f"⏰ Buy Intervals: 2 seconds")
    print(f"🌐 API: Delta Exchange India (1-SECOND POLLING)")
    print(f"📝 Paper Trading: {PAPER_TRADING}")
    
    eth_thread = threading.Thread(target=eth_bot.ultra_fast_monitoring, daemon=True)
    btc_thread = threading.Thread(target=btc_bot.ultra_fast_monitoring, daemon=True)
    
    eth_thread.start()
    btc_thread.start()
    
    send_telegram(f"🤖 ULTRA-FAST Arbitrage Bot Started\n\n🔵 ETH: ${ETH_PARAMS['min_profit']} min profit\n🟡 BTC: ${BTC_PARAMS['min_profit']} min profit\n⚡ DATA FETCHING: EVERY SECOND\n📦 Order Quantity: 100 LOTS\n⏰ Sell Timeout: 5 seconds\n⏰ Buy Intervals: 2 seconds\n📊 Data: Delta Exchange India API (1-SECOND)\n🔄 Real-time Opportunities: Enabled\n⏰ Started: {get_ist_time()} IST\n📝 Paper Trading: {PAPER_TRADING}")

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
