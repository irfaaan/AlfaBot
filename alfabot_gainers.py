#!/usr/bin/env python3
"""
AlfaBot - Professional MEXC Gainers Scanner & Telegram Signal Bot
Professional Scalper & Trader with 20+ Years Experience
Scans USDT pairs for significant pumps (10%+) across multiple timeframes
Sends high-quality signals to Telegram with AI analysis
"""

import os
import sys
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import json

from dotenv import load_dotenv
import ccxt.async_support as ccxt
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
import aiohttp
import requests

# Load environment
load_dotenv()

# ============================================
# CONFIGURATION
# ============================================

class Config:
    # Telegram
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    CHANNEL_ID = os.getenv("CHANNEL_ID", "@pers0naltesting")
    ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
    
    # MEXC
    MEXC_API_KEY = os.getenv("MEXC_API_KEY")
    MEXC_API_SECRET = os.getenv("MEXC_API_SECRET")
    
    # OpenRouter AI
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENROUTER_API_KEY_1")
    OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
    AI_MODEL = os.getenv("AI_MODEL", "nvidia/nemotron-3-ultra-550b-a55b:free")
    
    # Gainers Settings
    GAINERS_ENABLED = os.getenv("GAINERS_ENABLED", "true").lower() == "true"
    GAINERS_MIN_PUMP = float(os.getenv("GAINERS_MIN_PUMP", "10.0"))
    GAINERS_TIMEFRAMES = os.getenv("GAINERS_TIMEFRAMES", "1m,5m,15m,1h").split(",")
    GAINERS_SCAN_INTERVAL = int(os.getenv("GAINERS_SCAN_INTERVAL", "30"))
    GAINERS_MAX_PAIRS = int(os.getenv("GAINERS_MAX_PAIRS", "50"))
    GAINERS_MIN_VOLUME = float(os.getenv("GAINERS_MIN_VOLUME", "100000"))
    
    # Runtime toggles
    AI_ENABLED = os.getenv("AI_ENABLED", "true").lower() == "true"
    BROADCAST_ENABLED = os.getenv("BROADCAST_ENABLED", "true").lower() == "true"
    TRACKING_ENABLED = os.getenv("TRACKING_ENABLED", "true").lower() == "true"

# ============================================
# LOGGING
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('alfabot_gainers.log')
    ]
)
logger = logging.getLogger("AlfaBot-Gainers")

# ============================================
# DATA CLASSES
# ============================================

@dataclass
class PumpSignal:
    symbol: str
    timeframe: str
    pump_percent: float
    current_price: float
    volume_24h: float
    low_wick: float
    high_wick: float
    open_price: float
    close_price: float
    timestamp: datetime
    ai_opinion: Optional[str] = None
    confidence: float = 0.0
    entry_zone: str = ""
    target_zone: str = ""
    stop_loss: str = ""
    chances: str = ""
    candle_pattern: str = ""

# ============================================
# PROFESSIONAL SCALPER AI PROMPT
# ============================================

PROFESSIONAL_SCALPER_PROMPT = """You are a legendary professional scalper with 20+ years experience. ONLY output in the exact format. NO thinking, NO reasoning, NO extra text.

ANALYZE THIS PUMP (pure price action + volume + candle structure):

Symbol: {symbol}
Timeframe: {timeframe}
Pump: +{pump_percent:.2f}%
Price: {current_price}
Volume: ${volume_24h:,.0f}
Open: {open_price} | Close: {close_price}
Low Wick Ratio: {low_wick} | High Wick Ratio: {high_wick}

CANDLE STRUCTURE ANALYSIS:
- Body size vs previous implied movement
- Wick dominance (which wick is stronger)
- Volume context for the move
- Pump speed (how violent the move was)

OUTPUT EXACTLY:

**SIGNAL ANALYSIS**
**Confidence:** XX% 
**Candle Pattern:** [Strong Bullish Engulfing / Weak Continuation / Rejection at Highs / Trap Setup / Momentum Continuation]
**Wick Analysis:** Low: [weak/moderate/strong] | High: [rejection/continuation]
**Chances to Continue Up:** [Low/Medium/High] (X/10)
**Entry Recommendation:** From X.XXXXXX to X.XXXXXX
**Target 1 (Quick Scalp):** X.XXXXXX (+X%)
**Target 2 (Runner):** X.XXXXXX (+X%)
**Stop Loss:** X.XXXXXX (-X%)
**Risk/Reward:** 1:X.X
**Key Insight:** One powerful sentence about the candle structure and momentum.

Rules:
- Focus on candle body, wicks, and volume only
- Be brutally honest
- No indicators
- No extra text outside format"""

# ============================================
# MEXC GAINERS BOT CLASS
# ============================================

class MEXCGainersBot:
    def __init__(self):
        self.exchange = None
        self.app = None
        self.bot = None
        self.running = False
        self.scanning = False
        self.last_scan = None
        self.detected_pumps: Dict[str, datetime] = {}  # symbol -> last detection time
        self.stats = {
            "scans": 0,
            "signals_sent": 0,
            "pumps_detected": 0
        }
        self.min_pump = Config.GAINERS_MIN_PUMP
        self.scan_interval = Config.GAINERS_SCAN_INTERVAL
        
    async def initialize_exchange(self):
        """Initialize MEXC exchange with CCXT"""
        try:
            self.exchange = ccxt.mexc({
                'apiKey': Config.MEXC_API_KEY or None,
                'secret': Config.MEXC_API_SECRET or None,
                'sandbox': False,
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'spot',
                }
            })
            
            # Test connection
            await self.exchange.load_markets()
            logger.info(f"✅ Connected to MEXC - {len(self.exchange.markets)} markets loaded")
            return True
        except Exception as e:
            logger.error(f"❌ MEXC connection error: {e}")
            logger.info("💡 Tip: The bot works best when run on a VPS close to MEXC servers")
            return False

    async def initialize_telegram(self):
        """Initialize Telegram bot"""
        try:
            if not Config.TELEGRAM_TOKEN:
                logger.error("❌ TELEGRAM_TOKEN not found in .env")
                return False
                
            self.app = Application.builder().token(Config.TELEGRAM_TOKEN).build()
            self.bot = self.app.bot
            
            # Add handlers
            self.app.add_handler(CommandHandler("start", self.cmd_start))
            self.app.add_handler(CommandHandler("scan", self.cmd_scan))
            self.app.add_handler(CommandHandler("status", self.cmd_status))
            self.app.add_handler(CommandHandler("enable", self.cmd_enable))
            self.app.add_handler(CommandHandler("disable", self.cmd_disable))
            self.app.add_handler(CommandHandler("setpump", self.cmd_setpump))
            self.app.add_handler(CommandHandler("setinterval", self.cmd_setinterval))
            self.app.add_handler(CommandHandler("stats", self.cmd_stats))
            self.app.add_handler(CommandHandler("help", self.cmd_help))
            self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
            
            logger.info("✅ Telegram bot initialized")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize Telegram: {e}")
            return False

    # ============================================
    # TELEGRAM COMMANDS
    # ============================================

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start command"""
        user = update.effective_user
        msg = (
            f"🚀 **AlfaBot Gainers Scanner** activated!\n\n"
            f"Professional 20+ Year Scalper Mode: **ON**\n"
            f"Scanning MEXC USDT pairs every {self.scan_interval}s\n\n"
            f"Use /help to see all commands"
        )
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def cmd_scan(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Manual scan trigger"""
        if self.scanning:
            await update.message.reply_text("⏳ Scan already in progress...")
            return
            
        await update.message.reply_text("🔍 Starting manual scan for gainers...")
        asyncio.create_task(self.run_scan(manual=True, chat_id=update.effective_chat.id))

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show current status"""
        status = "🟢 RUNNING" if self.running else "🔴 STOPPED"
        scan_status = "🔄 SCANNING" if self.scanning else "⏸️ IDLE"
        
        msg = (
            f"**AlfaBot Gainers Status**\n\n"
            f"Bot: {status}\n"
            f"Scanner: {scan_status}\n"
            f"Min Pump: {self.min_pump}%\n"
            f"Scan Interval: {self.scan_interval}s\n"
            f"Last Scan: {self.last_scan.strftime('%H:%M:%S') if self.last_scan else 'Never'}\n"
            f"Total Scans: {self.stats['scans']}\n"
            f"Signals Sent: {self.stats['signals_sent']}\n"
            f"Pumps Detected: {self.stats['pumps_detected']}"
        )
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def cmd_enable(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enable scanning"""
        self.running = True
        await update.message.reply_text("✅ Gainers scanner **ENABLED**")

    async def cmd_disable(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Disable scanning"""
        self.running = False
        await update.message.reply_text("⛔ Gainers scanner **DISABLED**")

    async def cmd_setpump(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Set minimum pump percentage"""
        try:
            if context.args:
                new_pump = float(context.args[0])
                if 5 <= new_pump <= 100:
                    self.min_pump = new_pump
                    await update.message.reply_text(f"✅ Minimum pump set to **{new_pump}%**")
                else:
                    await update.message.reply_text("❌ Value must be between 5% and 100%")
            else:
                await update.message.reply_text(f"Current min pump: **{self.min_pump}%**\nUsage: /setpump 15")
        except ValueError:
            await update.message.reply_text("❌ Invalid number")

    async def cmd_setinterval(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Set scan interval"""
        try:
            if context.args:
                new_interval = int(context.args[0])
                if 10 <= new_interval <= 300:
                    self.scan_interval = new_interval
                    await update.message.reply_text(f"✅ Scan interval set to **{new_interval} seconds**")
                else:
                    await update.message.reply_text("❌ Interval must be 10-300 seconds")
            else:
                await update.message.reply_text(f"Current interval: **{self.scan_interval}s**\nUsage: /setinterval 30")
        except ValueError:
            await update.message.reply_text("❌ Invalid number")

    async def cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show statistics"""
        msg = (
            f"📊 **AlfaBot Statistics**\n\n"
            f"Total Scans: {self.stats['scans']}\n"
            f"Signals Sent: {self.stats['signals_sent']}\n"
            f"Pumps Detected: {self.stats['pumps_detected']}\n"
            f"Unique Symbols Tracked: {len(self.detected_pumps)}"
        )
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Help command"""
        msg = (
            "**AlfaBot Gainers - Commands**\n\n"
            "/start - Start the bot\n"
            "/scan - Manual scan now\n"
            "/status - Current status\n"
            "/enable - Enable scanning\n"
            "/disable - Disable scanning\n"
            "/setpump <X> - Set min pump % (default 10)\n"
            "/setinterval <sec> - Set scan interval\n"
            "/stats - Show statistics\n"
            "/help - This message\n\n"
            "**Professional Scalper Mode Active**"
        )
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle non-command messages"""
        # Could add chat functionality later
        pass

    # ============================================
    # CORE SCANNING LOGIC
    # ============================================

    async def get_usdt_pairs(self) -> List[str]:
        """Get all active USDT spot pairs from MEXC"""
        try:
            markets = self.exchange.markets
            usdt_pairs = []
            
            for symbol, market in markets.items():
                if (market.get('spot') and 
                    '/USDT' in symbol and 
                    market.get('active', True) and
                    not market.get('info', {}).get('isSpotTradingAllowed', True) == False):
                    
                    # Filter out some low quality pairs
                    if any(x in symbol for x in ['UP', 'DOWN', 'BULL', 'BEAR', '3L', '3S', '5L', '5S']):
                        continue
                    usdt_pairs.append(symbol)
            
            logger.info(f"Found {len(usdt_pairs)} USDT pairs")
            return usdt_pairs[:Config.GAINERS_MAX_PAIRS]  # Limit for performance
        except Exception as e:
            logger.error(f"Error fetching USDT pairs: {e}")
            return []

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 5) -> Optional[List]:
        """Fetch OHLCV data"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            return ohlcv
        except Exception as e:
            # logger.debug(f"OHLCV error for {symbol} {timeframe}: {e}")
            return None

    def calculate_pump(self, ohlcv: List) -> Tuple[float, float, float, float, float]:
        """Calculate pump percentage and wick data from OHLCV"""
        if not ohlcv or len(ohlcv) < 2:
            return 0.0, 0.0, 0.0, 0.0, 0.0
            
        # Latest candle
        latest = ohlcv[-1]
        open_price = latest[1]
        high = latest[2]
        low = latest[3]
        close = latest[4]
        
        if open_price == 0:
            return 0.0, 0.0, 0.0, 0.0, 0.0
            
        pump_percent = ((close - open_price) / open_price) * 100
        
        # Wick calculations
        body = abs(close - open_price)
        upper_wick = high - max(open_price, close)
        lower_wick = min(open_price, close) - low
        
        high_wick_ratio = upper_wick / body if body > 0 else 0
        low_wick_ratio = lower_wick / body if body > 0 else 0
        
        return pump_percent, open_price, close, low_wick_ratio, high_wick_ratio

    def detect_candle_pattern(self, ohlcv: List, pump_percent: float, low_wick: float, high_wick: float) -> str:
        """Detect pure price action candle patterns (no indicators)"""
        if not ohlcv or len(ohlcv) < 2:
            return "Unknown Structure"
            
        latest = ohlcv[-1]
        prev = ohlcv[-2] if len(ohlcv) > 1 else latest
        
        open_p = latest[1]
        high = latest[2]
        low = latest[3]
        close = latest[4]
        
        prev_close = prev[4]
        prev_open = prev[1]
        
        body = abs(close - open_p)
        total_range = high - low
        
        # Calculate body percentage of range
        body_pct = (body / total_range * 100) if total_range > 0 else 50
        
        # Determine pattern based on pure price action
        if pump_percent >= 20:
            if low_wick > 2.0 and high_wick < 0.5:
                return "Massive Bullish Hammer / Strong Rejection from Lows"
            elif high_wick > 1.5:
                return "Violent Spike with Heavy Upper Wick Rejection"
            else:
                return "Explosive Bullish Momentum (Very Strong)"
                
        elif pump_percent >= 15:
            if low_wick > 1.8 and high_wick < 0.6:
                return "Strong Bullish Rejection (Strong Lower Wick)"
            elif high_wick > 1.2:
                return "Violent Pump with Upper Rejection"
            elif body_pct > 80:
                return "Powerful Bullish Marubozu (Almost No Wicks)"
            else:
                return "Explosive Bullish Momentum"
                
        elif pump_percent >= 10:
            if body_pct > 75 and low_wick < 0.4:
                return "Strong Bullish Body (Minimal Lower Wick)"
            elif low_wick > 1.5:
                return "Bullish with Strong Support Wick"
            elif high_wick > 1.0:
                return "Bullish with Selling Pressure at Highs"
            elif body_pct > 65:
                return "Solid Bullish Continuation"
            else:
                return "Bullish Engulfing Style Move"
                
        else:
            if high_wick > 1.3 and low_wick < 0.5:
                return "Rejection at Resistance"
            elif low_wick > 1.8:
                return "Strong Support Test (Long Lower Wick)"
            elif body_pct > 70:
                return "Moderate Bullish Body"
            else:
                return "Moderate Bullish Move"

    async def get_ticker(self, symbol: str) -> Optional[Dict]:
        """Get current ticker data"""
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            return ticker
        except:
            return None

    async def get_ai_opinion(self, signal: PumpSignal) -> Optional[str]:
        """Get professional opinion from OpenRouter AI"""
        if not Config.AI_ENABLED or not Config.OPENROUTER_API_KEY:
            return None
            
        try:
            prompt = PROFESSIONAL_SCALPER_PROMPT.format(
                symbol=signal.symbol,
                timeframe=signal.timeframe,
                pump_percent=signal.pump_percent,
                current_price=signal.current_price,
                volume_24h=signal.volume_24h,
                open_price=signal.open_price,
                close_price=signal.close_price,
                low_wick=signal.low_wick,
                high_wick=signal.high_wick
            )
            
            headers = {
                "Authorization": f"Bearer {Config.OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://alfabot.ai",
                "X-Title": "AlfaBot Gainers"
            }
            
            payload = {
                "model": Config.AI_MODEL,
                "messages": [
                    {"role": "system", "content": "You are a 20+ year veteran crypto scalper. Be concise and professional."},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 600,
                "temperature": 0.3
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{Config.OPENROUTER_BASE_URL}/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=25)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data['choices'][0]['message']['content'].strip()
                    else:
                        logger.warning(f"OpenRouter error: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"AI opinion error: {e}")
            return None

    def parse_ai_signal(self, ai_text: str, signal: PumpSignal) -> PumpSignal:
        """Parse AI response into structured signal (more robust)"""
        if not ai_text:
            return signal
            
        try:
            # Clean the response - remove any thinking/reasoning
            clean_text = ai_text
            if "**SIGNAL ANALYSIS**" in clean_text:
                clean_text = clean_text.split("**SIGNAL ANALYSIS**")[1]
            
            lines = clean_text.split('\n')
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                    
                if 'Confidence:' in line:
                    try:
                        parts = line.split(':')
                        if len(parts) > 1:
                            conf_str = parts[1].replace('%', '').strip().split()[0]
                            conf = float(conf_str)
                            signal.confidence = min(max(conf, 40), 95)
                    except:
                        signal.confidence = 68.0
                        
                elif 'Chances to Continue' in line or 'Chances Up' in line:
                    if ':' in line:
                        signal.chances = line.split(':')[1].strip()
                    else:
                        signal.chances = "Medium (6/10)"
                        
                elif 'Entry Recommendation' in line or 'Entry:' in line:
                    if ':' in line:
                        signal.entry_zone = line.split(':')[1].strip()
                        
                elif 'Target 1' in line:
                    if ':' in line:
                        signal.target_zone = line.split(':')[1].strip()
                        
                elif 'Stop Loss' in line:
                    if ':' in line:
                        signal.stop_loss = line.split(':')[1].strip()
                        
                elif 'Key Insight' in line:
                    pass
                    
                elif 'Candle Pattern' in line:
                    # Optional: could store pattern if needed
                    pass
            
            # Smart fallbacks based on actual data
            if signal.confidence < 40:
                signal.confidence = 65.0
                
            if not signal.entry_zone or len(signal.entry_zone) < 3:
                signal.entry_zone = f"{signal.current_price:.8f} - {signal.current_price * 1.004:.8f}"
                
            if not signal.target_zone or len(signal.target_zone) < 3:
                target_pct = 8 + (signal.pump_percent * 0.3)
                signal.target_zone = f"{signal.current_price * (1 + target_pct/100):.8f} (+{target_pct:.0f}%)"
                
            if not signal.stop_loss or len(signal.stop_loss) < 3:
                signal.stop_loss = f"{signal.current_price * 0.935:.8f} (-6.5%)"
                
            if not signal.chances or len(signal.chances) < 3:
                if signal.pump_percent > 18:
                    signal.chances = "High (7/10)"
                elif signal.pump_percent > 12:
                    signal.chances = "Medium (6/10)"
                else:
                    signal.chances = "Low (4/10)"
                    
        except Exception as e:
            logger.error(f"AI parsing error: {e}")
            # Set safe defaults
            signal.confidence = 62.0
            signal.entry_zone = f"{signal.current_price:.8f}"
            signal.target_zone = f"{signal.current_price * 1.09:.8f} (+9%)"
            signal.stop_loss = f"{signal.current_price * 0.94:.8f} (-6%)"
            signal.chances = "Medium (5/10)"
            
        return signal

    async def send_signal_to_telegram(self, signal: PumpSignal):
        """Send professional signal to Telegram channel"""
        if not Config.BROADCAST_ENABLED or not self.bot:
            return
            
        try:
            # Format the signal professionally
            emoji = "🚀" if signal.pump_percent >= 20 else "📈"
            wick_emoji = "🟢" if signal.low_wick > 1.5 else "🟡" if signal.low_wick > 0.8 else "🔴"
            
            msg = (
                f"{emoji} **MEXC GAINER ALERT** {emoji}\n\n"
                f"**{signal.symbol}** | **{signal.timeframe}** | **+{signal.pump_percent:.1f}%**\n\n"
                f"💰 **Price:** ${signal.current_price:,.8f}\n"
                f"📊 **24h Vol:** ${signal.volume_24h:,.0f}\n\n"
                f"**CANDLE STRUCTURE**\n"
                f"Pattern: {getattr(signal, 'candle_pattern', 'Bullish Move')}\n"
                f"Low Wick: {wick_emoji} {signal.low_wick:.2f}x body\n"
                f"High Wick: {signal.high_wick:.2f}x body\n\n"
                f"**SCALPER ANALYSIS**\n"
                f"Confidence: **{signal.confidence:.0f}%**\n"
                f"Chances Up: **{signal.chances}**\n\n"
                f"**TRADE PLAN**\n"
                f"Entry: {signal.entry_zone}\n"
                f"Target: {signal.target_zone}\n"
                f"Stop Loss: {signal.stop_loss}\n\n"
                f"⏰ {signal.timestamp.strftime('%H:%M:%S')} | MEXC Spot\n"
                f"#Gainers #{signal.symbol.replace('/USDT', '')}"
            )
            
            # Add AI opinion if available
            if signal.ai_opinion:
                # Truncate if too long
                ai_part = signal.ai_opinion[:800] + "..." if len(signal.ai_opinion) > 800 else signal.ai_opinion
                msg += f"\n\n**AI SCALPER INSIGHT**\n{ai_part}"
            
            await self.bot.send_message(
                chat_id=Config.CHANNEL_ID,
                text=msg,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
            
            self.stats['signals_sent'] += 1
            logger.info(f"✅ Signal sent: {signal.symbol} +{signal.pump_percent:.1f}% ({signal.timeframe})")
            
        except Exception as e:
            logger.error(f"Failed to send Telegram signal: {e}")

    async def analyze_pair(self, symbol: str, timeframe: str) -> Optional[PumpSignal]:
        """Analyze a single pair for pumps"""
        try:
            ohlcv = await self.fetch_ohlcv(symbol, timeframe, limit=3)
            if not ohlcv:
                return None
                
            pump_percent, open_p, close_p, low_wick, high_wick = self.calculate_pump(ohlcv)
            
            if pump_percent < self.min_pump:
                return None
                
            # Get ticker for volume
            ticker = await self.get_ticker(symbol)
            volume_24h = ticker.get('quoteVolume', 0) if ticker else 0
            
            if volume_24h < Config.GAINERS_MIN_VOLUME:
                return None
                
            # Create signal
            candle_pattern = self.detect_candle_pattern(ohlcv, pump_percent, low_wick, high_wick)
            
            signal = PumpSignal(
                symbol=symbol,
                timeframe=timeframe,
                pump_percent=round(pump_percent, 2),
                current_price=round(close_p, 8),
                volume_24h=round(volume_24h, 0),
                low_wick=round(low_wick, 2),
                high_wick=round(high_wick, 2),
                open_price=round(open_p, 8),
                close_price=round(close_p, 8),
                timestamp=datetime.now()
            )
            # Store candle pattern for AI prompt
            signal.candle_pattern = candle_pattern
            
            # Get AI opinion
            if Config.AI_ENABLED:
                ai_text = await self.get_ai_opinion(signal)
                if ai_text:
                    signal = self.parse_ai_signal(ai_text, signal)
                    signal.ai_opinion = ai_text
            
            return signal
            
        except Exception as e:
            # logger.debug(f"Analysis error {symbol}: {e}")
            return None

    async def run_scan(self, manual: bool = False, chat_id: Optional[int] = None):
        """Main scanning function"""
        if self.scanning:
            return
            
        self.scanning = True
        start_time = time.time()
        
        try:
            logger.info(f"🔍 Starting scan (manual={manual})...")
            self.stats['scans'] += 1
            
            usdt_pairs = await self.get_usdt_pairs()
            if not usdt_pairs:
                logger.warning("No USDT pairs found")
                return
                
            signals = []
            timeframes = Config.GAINERS_TIMEFRAMES
            
            for symbol in usdt_pairs:
                for tf in timeframes:
                    # Skip if recently detected
                    key = f"{symbol}_{tf}"
                    if key in self.detected_pumps:
                        last_time = self.detected_pumps[key]
                        if (datetime.now() - last_time).total_seconds() < 1800:  # 30 min cooldown
                            continue
                    
                    signal = await self.analyze_pair(symbol, tf)
                    if signal:
                        signals.append(signal)
                        self.detected_pumps[key] = datetime.now()
                        self.stats['pumps_detected'] += 1
                        
                        # Send immediately
                        await self.send_signal_to_telegram(signal)
                        
                        # Small delay to avoid rate limits
                        await asyncio.sleep(0.8)
            
            duration = time.time() - start_time
            logger.info(f"✅ Scan complete in {duration:.1f}s - Found {len(signals)} pumps")
            
            if manual and chat_id and self.bot:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=f"✅ Manual scan complete\nFound {len(signals)} pumps in {duration:.1f}s"
                )
                
        except Exception as e:
            logger.error(f"Scan error: {e}")
        finally:
            self.scanning = False
            self.last_scan = datetime.now()

    # ============================================
    # MAIN LOOP
    # ============================================

    async def main_loop(self):
        """Main scanning loop"""
        logger.info("🚀 AlfaBot Gainers starting main loop...")
        
        while self.running:
            try:
                if Config.GAINERS_ENABLED:
                    await self.run_scan()
                
                # Wait for next scan
                await asyncio.sleep(self.scan_interval)
                
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(60)

    async def start(self):
        """Start the bot"""
        logger.info("🚀 Starting AlfaBot Gainers...")
        
        # Initialize components
        if not await self.initialize_exchange():
            logger.error("Failed to initialize exchange. Exiting.")
            return
            
        if not await self.initialize_telegram():
            logger.error("Failed to initialize Telegram. Exiting.")
            return
        
        self.running = True
        
        # Start Telegram polling
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling()
        
        logger.info("✅ AlfaBot Gainers is LIVE and scanning!")
        
        # Send startup notification
        try:
            await self.bot.send_message(
                chat_id=Config.CHANNEL_ID,
                text="🚀 **AlfaBot Gainers Scanner** is now ONLINE\n\n"
                     "Professional 20+ Year Scalper Mode: **ACTIVE**\n"
                     f"Scanning every {self.scan_interval} seconds for 10%+ pumps"
            )
        except:
            pass
        
        # Start main scanning loop
        await self.main_loop()

    async def stop(self):
        """Stop the bot"""
        self.running = False
        if self.exchange:
            await self.exchange.close()
        if self.app:
            await self.app.stop()
        logger.info("🛑 AlfaBot Gainers stopped")


# ============================================
# ENTRY POINT
# ============================================

async def main():
    bot = MEXCGainersBot()
    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await bot.stop()

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════╗
║           AlfaBot Gainers - Professional MEXC Scanner        ║
║              20+ Years Scalper Experience Mode               ║
╚══════════════════════════════════════════════════════════════╝
    """)
    asyncio.run(main())
