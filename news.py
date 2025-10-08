#!/usr/bin/env python3
"""
MONGODB ATLAS TELEGRAM NEWS SCRAPER v6.0.0
High-performance news scraping with MongoDB Atlas backend
"""

import os
import re
import json
import ast
import asyncio
import logging
import gc
import time
import html
import hashlib
import functools
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import List, Dict, Optional, Any, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict, deque
from functools import lru_cache
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

import pymongo
from pymongo import UpdateOne, UpdateMany, DeleteOne, InsertOne, ReplaceOne
from pymongo.errors import ServerSelectionTimeoutError, ConfigurationError
from motor.motor_asyncio import AsyncIOMotorClient as AsyncMongoClient
import aiohttp
import google.generativeai as genai
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telethon import TelegramClient, events
from telethon.errors import ChannelPrivateError
from telethon.sessions import StringSession
from tenacity import retry, stop_after_attempt, wait_exponential
import psutil

# Telegram Bot imports
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError, TimedOut, NetworkError

# Configuration loaded from .env file
TELEGRAM_API_ID = int(os.getenv('TELEGRAM_API_ID', '0'))
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH', '')

# MongoDB Atlas Configuration
MONGODB_URI = os.getenv('MONGODB_URI', '')
DATABASE_NAME = os.getenv('DATABASE_NAME', 'blade_news')
COLLECTIONS = {
    "content": os.getenv('COLLECTION_CONTENT', 'content_items'),
    "metadata": os.getenv('COLLECTION_METADATA', 'scraper_metadata'),
    "cache": os.getenv('COLLECTION_CACHE', 'duplicate_cache'),
}

# Gemini Configuration
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')

# Batch processing configuration
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '8'))
CONCURRENT_API_CALLS = int(os.getenv('CONCURRENT_API_CALLS', '4'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
TIMEOUT_SECONDS = int(os.getenv('TIMEOUT_SECONDS', '30'))

# Dynamic Scraping Configuration
INITIAL_SCRAPE_HOURS = int(os.getenv('INITIAL_SCRAPE_HOURS', '1'))
REAL_TIME_INTERVAL_MINUTES = int(os.getenv('REAL_TIME_INTERVAL_MINUTES', '30'))

# Channel Usernames Configuration
_channel_usernames = os.getenv('CHANNEL_USERNAMES')
if _channel_usernames:
    try:
        CHANNEL_USERNAMES = json.loads(_channel_usernames)
    except json.JSONDecodeError:
        try:
            CHANNEL_USERNAMES = ast.literal_eval(_channel_usernames)
        except (ValueError, SyntaxError):
            raise ValueError("CHANNEL_USERNAMES must be a valid JSON array or Python list in .env file")
else:
    CHANNEL_USERNAMES = []

# Duplicate detection configuration
DUPLICATE_THRESHOLD = float(os.getenv('DUPLICATE_THRESHOLD', '0.70'))
MAX_COMPARISON_NEWS = int(os.getenv('MAX_COMPARISON_NEWS', '200'))
SIMILARITY_THRESHOLD = float(os.getenv('SIMILARITY_THRESHOLD', '0.75'))
DUPLICATE_CACHE_TTL = int(os.getenv('DUPLICATE_CACHE_TTL', '36000'))
RATE_LIMIT_CACHE_TTL = int(os.getenv('RATE_LIMIT_CACHE_TTL', '30'))

# Session file
SESSION_FILE = os.getenv('SESSION_FILE', 'telegram_session.txt')

# Bot Configuration
BOT_TOKEN = os.getenv('BOT_TOKEN', '')
CHANNEL_USERNAME = os.getenv('CHANNEL_USERNAME', '')
BOT_SEND_INTERVAL_MINUTES = int(os.getenv('BOT_SEND_INTERVAL_MINUTES', '2'))
BOT_MAX_MESSAGES_PER_MINUTE = int(os.getenv('BOT_MAX_MESSAGES_PER_MINUTE', '20'))

# Bot configuration

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("news_scraper")

# ==================== MODEL CONFIGURATION ====================
@dataclass
class ModelConfig:
    """Configuration for a Gemini model"""
    name: str
    rpm: int
    rpd: int
    priority: int
    
    def __post_init__(self):
        # Add safety buffer (80% of actual limits to prevent errors)
        self.safe_rpm = int(self.rpm * 0.8)
        self.safe_rpd = int(self.rpd * 0.8)

# Gemini model configurations
GEMINI_MODELS = [
    ModelConfig("gemini-2.5-flash", rpm=8, rpd=200, priority=1),
    ModelConfig("gemini-2.5-flash-lite", rpm=12, rpd=800, priority=2),
    ModelConfig("gemini-2.0-flash", rpm=12, rpd=150, priority=3),
    ModelConfig("gemini-2.0-flash-lite", rpm=25, rpd=180, priority=4),
    ModelConfig("gemma-3-27b-it", rpm=25, rpd=12000, priority=5),
]

MODEL_CONFIGS = {model.name: model for model in GEMINI_MODELS}

# Pre-compiled regex patterns
class OptimizedPatterns:
    """Pre-compiled regex patterns for fast text processing"""
    
    def __init__(self):
        # URLs
        self.url_pattern = re.compile(r'https?://[^\s]+')
        
        # Markdown links
        self.markdown_link_pattern = re.compile(r'\[([^\]]+)\]\([^)]+\)')
        
        # Emojis
        self.emoji_pattern = re.compile(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF]+')
        
        # Whitespace
        self.whitespace_pattern = re.compile(r'\s+')
        
        # Object references
        self.object_patterns = [
            re.compile(r'<.*object at 0x[0-9a-f]+>', re.IGNORECASE),
            re.compile(r'<google\.generativeai\.types\.generation_types\.GenerateContentResponse', re.IGNORECASE),
            re.compile(r'<google\.generativeai\..*>', re.IGNORECASE),
            re.compile(r'<generativeai\..*>', re.IGNORECASE),
            re.compile(r'GenerateContentResponse object', re.IGNORECASE),
            re.compile(r'response object', re.IGNORECASE),
        ]
        
        # Informal patterns
        self.informal_patterns = [
            re.compile(r'\b(breaking|BREAKING)\b', re.IGNORECASE),
            re.compile(r'\b(just|just now)\b', re.IGNORECASE), 
            re.compile(r'\b(huge|big|massive)\b', re.IGNORECASE),
            re.compile(r'\b(check out|look at)\b', re.IGNORECASE),
            re.compile(r'\b(this is|that is)\b', re.IGNORECASE),
            re.compile(r'\b(ðŸš€|ðŸ“ˆ|ðŸ’¥|ðŸ”¥|âš¡)', re.IGNORECASE),
            re.compile(r'\b(btw|fyi|imo|tbh)\b', re.IGNORECASE),
            re.compile(r'\b(omg|wow|amazing|incredible)\b', re.IGNORECASE)
        ]
        
        # Contractions
        self.contractions = {
            re.compile(r"\bdon't\b", re.IGNORECASE): "do not",
            re.compile(r"\bcan't\b", re.IGNORECASE): "cannot", 
            re.compile(r"\bwon't\b", re.IGNORECASE): "will not",
            re.compile(r"\bhasn't\b", re.IGNORECASE): "has not",
            re.compile(r"\bhaven't\b", re.IGNORECASE): "have not",
            re.compile(r"\bdoesn't\b", re.IGNORECASE): "does not"
        }
        
        # Markdown escape characters
        self.markdown_chars = ['\\', '_', '*', '[', ']', '(', ')', '~', '`', 
                              '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']

# Global instance for reuse
PATTERNS = OptimizedPatterns()

# Text extraction and validation

@lru_cache(maxsize=1000)
def fast_validate_text_content(text_hash: str, text: str) -> Tuple[bool, str]:
    """
    Cached text validation
    Returns: (is_valid, validated_text_or_error_message)
    """
    if not isinstance(text, str):
        return False, f"Content is not string: {type(text)}"
    
    # Check for object reference strings using pre-compiled patterns
    for pattern in PATTERNS.object_patterns:
        if pattern.search(text):
            return False, f"Object reference detected: {pattern.pattern}"
    
    # Check minimum length
    if len(text.strip()) < 5:
        return False, f"Text too short: {len(text.strip())} chars"
    
    # Check for suspicious API-related content
    suspicious_phrases = ["generate_content", "api response", "model response", 
                         "text property", "candidates[0]", "parts[0]"]
    
    lower_text = text.lower()
    for phrase in suspicious_phrases:
        if phrase in lower_text and len(text) < 100:
            return False, f"Suspicious API content: {phrase}"
    
    return True, text.strip()

def optimized_extract_text_from_response(response) -> Optional[str]:
    """
    Extract text from API response
    """
    if response is None:
        return None
    
    # Check if response is already a string
    if isinstance(response, str):
        text_hash = hashlib.md5(response.encode()).hexdigest()
        is_valid, result = fast_validate_text_content(text_hash, response)
        return result if is_valid else None
    
    # Validate response object has expected structure
    if not hasattr(response, '__dict__'):
        return None
    
    # METHOD 1: Direct .text property (most effective method)
    try:
        if hasattr(response, 'text') and response.text is not None:
            if isinstance(response.text, str) and len(response.text.strip()) > 0:
                extracted_text = response.text.strip()
                text_hash = hashlib.md5(extracted_text.encode()).hexdigest()
                is_valid, validated_text = fast_validate_text_content(text_hash, extracted_text)
                return validated_text if is_valid else None
    except Exception:
        pass
    
    return None

def optimized_safe_message_for_telegram(text: Any) -> str:
    """
    Sanitize text for Telegram
    """
    if text is None:
        return "💥 Error: No content available"
    
    text_str = str(text) if not isinstance(text, str) else text
    text_hash = hashlib.md5(text_str.encode()).hexdigest()
    
    # Use cached validation
    is_valid, result = fast_validate_text_content(text_hash, text_str)
    if not is_valid:
        return f"💥 Error: Content validation failed - {result}"
    
    # Fast cleaning with pre-compiled patterns
    clean_text = PATTERNS.url_pattern.sub('', result)
    clean_text = PATTERNS.whitespace_pattern.sub(' ', clean_text).strip()
    
    # Truncate if too long
    if len(clean_text) > 2500:
        clean_text = clean_text[:2497] + "..."
    
    return clean_text if len(clean_text.strip()) >= 10 else "💥 Error: Content too short after cleaning"

# Duplicate detection cache

class SmartMongoCache:
    """MongoDB-based caching for duplicate detection"""
    
    def __init__(self, mongo_manager, ttl: int = DUPLICATE_CACHE_TTL):
        self.mongo_manager = mongo_manager
        self.ttl = ttl
        self.local_cache = {}
        self.cache_timestamps = {}
    
    def _generate_content_hash(self, text: str) -> str:
        """Generate hash for content"""
        normalized = PATTERNS.whitespace_pattern.sub(' ', text.lower().strip())
        return hashlib.md5(normalized.encode()).hexdigest()
    
    async def get_cached_duplicate_result(self, text: str) -> Optional[Tuple[str, float]]:
        """Get cached duplicate result"""
        content_hash = self._generate_content_hash(text)
        
        # Check local cache first
        if content_hash in self.local_cache:
            timestamp = self.cache_timestamps.get(content_hash, 0)
            if time.time() - timestamp < self.ttl:
                return self.local_cache[content_hash]
            else:
                del self.local_cache[content_hash]
                del self.cache_timestamps[content_hash]
        
        # Check MongoDB cache
        try:
            cache_collection = self.mongo_manager.db[COLLECTIONS["cache"]]
            cached_doc = await cache_collection.find_one({"content_hash": content_hash})
            
            if cached_doc:
                decision = cached_doc.get("decision", "NEW")
                confidence = cached_doc.get("confidence", 0.5)
                
                # Update local cache
                self.local_cache[content_hash] = (decision, confidence)
                self.cache_timestamps[content_hash] = time.time()
                
                return (decision, confidence)
        except Exception:
            pass
        
        return None
    
    async def cache_duplicate_result(self, text: str, decision: str, confidence: float):
        """Cache duplicate result in MongoDB"""
        content_hash = self._generate_content_hash(text)
        
        cache_data = {
            "content_hash": content_hash,
            "decision": decision,
            "confidence": confidence,
            "created_at": datetime.now(timezone.utc)
        }
        
        # Store in MongoDB
        try:
            cache_collection = self.mongo_manager.db[COLLECTIONS["cache"]]
            await cache_collection.update_one(
                {"content_hash": content_hash},
                {"$set": cache_data},
                upsert=True
            )
        except Exception:
            pass
        
        # Store in local cache
        self.local_cache[content_hash] = (decision, confidence)
        self.cache_timestamps[content_hash] = time.time()
    


# ==================== IN-MEMORY RATE LIMIT CACHE ====================

class RateLimitCache:
    """In-memory cache for rate limit status"""
    
    def __init__(self, ttl: int = RATE_LIMIT_CACHE_TTL):
        self.ttl = ttl
        self.cache = {}
        self.timestamps = {}
    
    def get_cached_rate_status(self, model_name: str) -> Optional[Tuple[bool, Dict]]:
        """Get cached rate limit status"""
        if model_name in self.cache:
            timestamp = self.timestamps.get(model_name, 0)
            if time.time() - timestamp < self.ttl:
                return self.cache[model_name]
            else:
                # Expired
                del self.cache[model_name]
                del self.timestamps[model_name]
        return None
    
    def cache_rate_status(self, model_name: str, can_use: bool, status: Dict):
        """Cache rate limit status"""
        self.cache[model_name] = (can_use, status)
        self.timestamps[model_name] = time.time()
    
    def clear_expired(self):
        """Clear expired cache entries"""
        current_time = time.time()
        expired_keys = [
            key for key, timestamp in self.timestamps.items() 
            if current_time - timestamp >= self.ttl
        ]
        for key in expired_keys:
            self.cache.pop(key, None)
            self.timestamps.pop(key, None)

# HTTP connection pooling

class OptimizedHTTPSession:
    """HTTP connection pooling for faster API calls"""
    
    def __init__(self):
        self.connector = None
        self.session = None
    
    async def __aenter__(self):
        self.connector = aiohttp.TCPConnector(
            limit=50,  # Total connection limit
            limit_per_host=10,  # Per-host connection limit
            ttl_dns_cache=300,  # DNS cache TTL
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
        self.session = aiohttp.ClientSession(
            connector=self.connector,
            timeout=timeout,
            headers={'User-Agent': 'OptimizedTelegramScraper/5.0'}
        )
        return self.session
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        if self.connector:
            await self.connector.close()

# Gemini rate limiter

class OptimizedGeminiRateLimiter:
    """Rate limiter with in-memory tracking"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        genai.configure(api_key=api_key)
        
        # Use in-memory rate tracking only
        self.rate_tracking = defaultdict(lambda: defaultdict(int))
        self.rate_cache = RateLimitCache()
        
        # Initialize models
        self.models = sorted(GEMINI_MODELS, key=lambda x: x.priority)
        self.model_instances = {}
        
        for model_config in self.models:
            try:
                generation_config = {
                    "max_output_tokens": 2048,
                    "temperature": 0.3,
                    "top_p": 0.8,
                }
                self.model_instances[model_config.name] = genai.GenerativeModel(
                    model_config.name,
                    generation_config=generation_config
                )
                logger.info(f"✅ Initialized model: {model_config.name}")
            except Exception as e:
                logger.error(f"💥 Failed to initialize {model_config.name}: {e}")
        
        self.current_model = self.models[0].name if self.models else None
    
    def _get_current_usage(self, model_name: str) -> Dict[str, int]:
        """Get current usage from in-memory tracking"""
        now = datetime.now(timezone.utc)
        date_key = now.strftime("%Y-%m-%d")
        minute_key = now.strftime("%Y-%m-%d-%H-%M")
        
        return {
            "daily_requests": self.rate_tracking[model_name][f"daily_{date_key}"],
            "minute_requests": self.rate_tracking[model_name][f"minute_{minute_key}"]
        }
    
    def _check_rate_limits_cached(self, model_name: str) -> Tuple[bool, Dict[str, Any]]:
        """Check rate limits with caching"""
        # Check cache first
        cached_result = self.rate_cache.get_cached_rate_status(model_name)
        if cached_result:
            return cached_result
        
        # Not cached - check in-memory tracking
        if model_name not in MODEL_CONFIGS:
            return False, {"error": f"Unknown model: {model_name}"}
        
        config = MODEL_CONFIGS[model_name]
        usage = self._get_current_usage(model_name)
        
        # Check all rate limits
        limits_status = {
            "daily_requests": {
                "current": usage["daily_requests"],
                "limit": config.safe_rpd,
                "exceeded": usage["daily_requests"] >= config.safe_rpd
            },
            "minute_requests": {
                "current": usage["minute_requests"],
                "limit": config.safe_rpm,
                "exceeded": usage["minute_requests"] >= config.safe_rpm
            },
        }
        
        any_exceeded = any(status["exceeded"] for status in limits_status.values())
        can_use = not any_exceeded
        
        status = {
            "model": model_name,
            "can_use": can_use,
            "limits": limits_status,
            "priority": config.priority
        }
        
        # Cache the result
        self.rate_cache.cache_rate_status(model_name, can_use, status)
        
        return can_use, status
    
    def _find_available_model(self) -> Optional[Tuple[str, Dict]]:
        """Find the best available model that's within rate limits"""
        for model_config in self.models:
            if model_config.name not in self.model_instances:
                continue
                
            can_use, status = self._check_rate_limits_cached(model_config.name)
            if can_use:
                return model_config.name, status
        
        return None
    
    def _increment_usage(self, model_name: str):
        """Increment in-memory usage counters"""
        now = datetime.now(timezone.utc)
        date_key = now.strftime("%Y-%m-%d")
        minute_key = now.strftime("%Y-%m-%d-%H-%M")
        
        self.rate_tracking[model_name][f"daily_{date_key}"] += 1
        self.rate_tracking[model_name][f"minute_{minute_key}"] += 1
        
        # Clean up old entries periodically
        self._cleanup_old_entries()
        
        # Invalidate cache for this model
        self.rate_cache.cache.pop(model_name, None)
        self.rate_cache.timestamps.pop(model_name, None)
    
    def _cleanup_old_entries(self):
        """Clean up old rate tracking entries"""
        # This is a simple cleanup - in production you might want more sophisticated cleanup
        pass
    
    async def generate_content_optimized(
        self,
        prompt: str,
        preferred_model: Optional[str] = None,
        max_wait_seconds: int = 180,
        context: str = "general"
    ) -> Dict[str, Any]:
        """Optimized content generation with caching and connection pooling"""
        start_time = time.time()
        
        # Find available model
        model_result = self._find_available_model()
        if not model_result:
            # Clear expired cache entries and try again
            self.rate_cache.clear_expired()
            model_result = self._find_available_model()
            if not model_result:
                raise Exception("No models available")
        
        selected_model, model_status = model_result
        
        # Generate content with selected model
        try:
            model_instance = self.model_instances[selected_model]
            response = model_instance.generate_content(prompt)
            
            # Use optimized text extraction
            response_text = optimized_extract_text_from_response(response)
            
            if response_text is None:
                raise ValueError(f"Text extraction failed for {selected_model}")
            
            # Context-specific validation
            if context == "paraphrase" and len(response_text) < 20:
                raise ValueError(f"Paraphrase response too short: {len(response_text)} chars")
            
            # Success - increment usage counters
            self._increment_usage(selected_model)
            self.current_model = selected_model
            
            generation_time = time.time() - start_time
            
            return {
                "success": True,
                "text": response_text,
                "model_used": selected_model,
                "model_priority": MODEL_CONFIGS[selected_model].priority,
                "generation_time_seconds": generation_time,
                "context": context,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            # Try one fallback model
            remaining_models = [m for m in self.models 
                              if m.name != selected_model and m.name in self.model_instances]
            
            for fallback_model in remaining_models[:1]:  # Try one fallback
                can_use, status = self._check_rate_limits_cached(fallback_model.name)
                if can_use:
                    try:
                        fallback_instance = self.model_instances[fallback_model.name]
                        response = fallback_instance.generate_content(prompt)
                        response_text = optimized_extract_text_from_response(response)
                        
                        if response_text is None:
                            continue
                        
                        self._increment_usage(fallback_model.name)
                        generation_time = time.time() - start_time
                        
                        return {
                            "success": True,
                            "text": response_text,
                            "model_used": fallback_model.name,
                            "model_priority": fallback_model.priority,
                            "generation_time_seconds": generation_time,
                            "fallback_reason": f"Primary model {selected_model} failed",
                            "context": context,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                    except Exception:
                        continue
            
            raise Exception(f"All models failed. Primary error: {str(e)}")
    


# Batch content processor

class BatchContentProcessor:
    """Batch processing engine for high throughput"""
    
    def __init__(self, api_key: str):
        self.rate_limiter = OptimizedGeminiRateLimiter(api_key)
        self.duplicate_cache = None  # Will be set by main class
        
        # Optimized prompts
        self.paraphrase_prompt = """
You are a professional financial news editor. Transform the text into professional financial news format.

REQUIREMENTS:
1. Remove URLs, emojis, hashtags, informal language
2. Preserve ALL factual information, numbers, dates, technical terms
3. Use professional, clear language
4. Maintain original meaning and context
5. Return ONLY the paraphrased text - no commentary
6. 20-2000 characters

INPUT: "{text}"
OUTPUT:"""

        self.duplicate_check_prompt = """
Compare NEW MESSAGE with RECENT NEWS. Determine if NEW contains substantially new information.

NEW MESSAGE: {new_message}
RECENT NEWS: {recent_news}

RESPONSE: NEW, DUPLICATE, or UPDATE"""
    
    async def process_single_message_optimized(self, text: str, recent_news: List[Dict]) -> Optional[Dict[str, Any]]:
        """Process single message"""
        try:
            # Fast text cleanup with pre-compiled patterns
            cleaned_text = self._fast_text_cleanup(text)
            if len(cleaned_text) < 20:
                return None
            
            # Check duplicate cache first
            cached_duplicate = await self.duplicate_cache.get_cached_duplicate_result(cleaned_text) if self.duplicate_cache else None
            
            if cached_duplicate:
                decision, confidence = cached_duplicate
            else:
                # Check for duplicates with API
                decision, confidence = await self._check_for_duplicate_fast(cleaned_text, recent_news)
                if self.duplicate_cache:
                    await self.duplicate_cache.cache_duplicate_result(cleaned_text, decision, confidence)
            
            # Skip duplicates
            if decision == "DUPLICATE" and confidence >= DUPLICATE_THRESHOLD:
                return {
                    "original_text": text,
                    "paraphrased_text": "[DUPLICATE] " + cleaned_text,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                    "processor": "batch_optimized",
                    "duplicate_decision": decision,
                    "duplicate_confidence": confidence
                }
            
            # Process with Gemini
            prompt = self.paraphrase_prompt.format(text=cleaned_text)
            
            result = await self.rate_limiter.generate_content_optimized(
                prompt=prompt,
                max_wait_seconds=120,
                context="paraphrase"
            )
            
            if not result["success"]:
                return None
            
            # Fast response cleaning
            raw_paraphrased = result["text"]
            cleaned_paraphrased = self._fast_clean_response(raw_paraphrased)
            is_valid, validated_paraphrased = self._fast_validate_paraphrase(cleaned_text, cleaned_paraphrased)
            
            if not is_valid:
                validated_paraphrased = self._fast_fallback_paraphrase(cleaned_text)
            
            return {
                "original_text": text,
                "paraphrased_text": validated_paraphrased,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "processor": "batch_optimized",
                "model_used": result["model_used"],
                "model_priority": result["model_priority"],
                "generation_time": result["generation_time_seconds"],
                "duplicate_decision": decision,
                "duplicate_confidence": confidence
            }
            
        except Exception as e:
            logger.error(f"💥 Batch processing error: {e}")
            return None
    
    async def process_batch_concurrent(self, messages: List[Tuple[str, Any]], recent_news: List[Dict]) -> List[Dict]:
        """Process batch with ENHANCED duplicate detection"""
        if not messages:
            return []

        unique_messages = {}
        filtered_messages = []
        
        for msg_data in messages:
            text, _ = msg_data
            cleaned = self._fast_text_cleanup(text)
            if len(cleaned) < 20:
                continue
            
            content_hash = hashlib.md5(
                re.sub(r'[^\w\s]', '', cleaned.lower().strip()).encode('utf-8')
            ).hexdigest()
            
            if content_hash not in unique_messages:
                unique_messages[content_hash] = msg_data
                filtered_messages.append(msg_data)
            else:
                logger.info(f"⏭️ Intra-batch duplicate: {cleaned[:50]}...")
        
        if not filtered_messages:
            logger.info("⏭️ All messages in batch were duplicates")
            return []
        
        logger.info(f"✅ Filtered batch: {len(messages)} → {len(filtered_messages)} unique messages")
        
        recent_hashes = set()
        for news in recent_news[:200]:
            news_text = news.get("paraphrased_text", news.get("original_text", ""))
            if news_text:
                news_hash = hashlib.md5(
                    re.sub(r'[^\w\s]', '', news_text.lower().strip()).encode('utf-8')
                ).hexdigest()
                recent_hashes.add(news_hash)
        
        final_messages = []
        for msg_data in filtered_messages:
            text, _ = msg_data
            cleaned = self._fast_text_cleanup(text)
            content_hash = hashlib.md5(
                re.sub(r'[^\w\s]', '', cleaned.lower().strip()).encode('utf-8')
            ).hexdigest()
            
            if content_hash not in recent_hashes:
                final_messages.append(msg_data)
            else:
                logger.info(f"⏭️ Duplicate against recent news: {cleaned[:50]}...")
        
        logger.info(f"✅ Final batch: {len(final_messages)} messages after recent news filter")
        
        semaphore = asyncio.Semaphore(CONCURRENT_API_CALLS)
        
        async def process_with_semaphore(msg_data):
            async with semaphore:
                text, _ = msg_data
                return await self.process_single_message_optimized(text, recent_news)
        
        results = []
        for i in range(0, len(final_messages), BATCH_SIZE):
            batch = final_messages[i:i + BATCH_SIZE]
            batch_results = await asyncio.gather(*[process_with_semaphore(msg) for msg in batch], return_exceptions=True)
            
            valid_results = [r for r in batch_results if r is not None and not isinstance(r, Exception)]
            results.extend(valid_results)
            
            if i + BATCH_SIZE < len(final_messages):
                await asyncio.sleep(0.5)
        
        return results
    
    def _fast_text_cleanup(self, text: str) -> str:
        """Fast text cleanup using pre-compiled patterns"""
        # Use pre-compiled patterns
        text = PATTERNS.url_pattern.sub('', text)
        text = PATTERNS.markdown_link_pattern.sub(r'\1', text)
        text = PATTERNS.emoji_pattern.sub(' ', text)
        text = PATTERNS.whitespace_pattern.sub(' ', text).strip()
        return text
    
    async def _check_for_duplicate_fast(self, new_text: str, recent_news: List[Dict]) -> Tuple[str, float]:
        """Fast duplicate checking"""
        if not recent_news:
            return "NEW", 1.0
        
        try:
            # Limit recent news
            recent_news_formatted = []
            for i, news in enumerate(recent_news[:min(50, MAX_COMPARISON_NEWS)], 1):
                news_text = news.get("paraphrased_text", news.get("original_text", ""))
                if news_text:
                    recent_news_formatted.append(f"{i}. {news_text}")
            
            if not recent_news_formatted:
                return "NEW", 1.0
            
            recent_news_str = "\n".join(recent_news_formatted)
            prompt = self.duplicate_check_prompt.format(
                new_message=new_text,
                recent_news=recent_news_str
            )
            
            result = await self.rate_limiter.generate_content_optimized(
                prompt=prompt,
                max_wait_seconds=30,
                context="duplicate_check"
            )
            
            if result["success"]:
                decision = result["text"].strip().upper()
                if "NEW" in decision:
                    return "NEW", 0.9
                elif "DUPLICATE" in decision:
                    return "DUPLICATE", 0.8
                elif "UPDATE" in decision:
                    return "UPDATE", 0.7
            
            return "NEW", 0.5
            
        except Exception:
            return "NEW", 0.5
    
    def _fast_clean_response(self, text: str) -> str:
        """Fast response cleaning"""
        text = re.sub(r'^["\']|["\']$', '', text.strip())
        return text.strip()
    
    def _fast_validate_paraphrase(self, original: str, paraphrased: str) -> Tuple[bool, str]:
        """Fast paraphrase validation"""
        # Remove common prefixes quickly
        unwanted_prefixes = [
            "Here's a professional paraphrase:",
            "Professional paraphrase:",
            "Paraphrased text:",
        ]
        
        cleaned = paraphrased
        for prefix in unwanted_prefixes:
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix):].strip()
                break
        
        # Quick checks
        if len(cleaned) < 20:
            return False, "Too short"
        
        if cleaned.lower().strip() == original.lower().strip():
            return False, "Identical to original"
        
        return True, cleaned
    
    def _fast_fallback_paraphrase(self, text: str) -> str:
        """Fast rule-based fallback paraphrasing"""
        cleaned = text
        
        # Remove informal patterns using pre-compiled regex
        for pattern in PATTERNS.informal_patterns:
            cleaned = pattern.sub('', cleaned)
        
        # Replace contractions
        for pattern, replacement in PATTERNS.contractions.items():
            cleaned = pattern.sub(replacement, cleaned)
        
        # Clean whitespace and capitalize
        cleaned = PATTERNS.whitespace_pattern.sub(' ', cleaned).strip()
        if cleaned and not cleaned[0].isupper():
            cleaned = cleaned[0].upper() + cleaned[1:]
        
        return cleaned if len(cleaned) > 20 else text

# MongoDB manager

class OptimizedMongoManager:
    """MongoDB Atlas manager with async operations"""
    
    def __init__(self, connection_uri: str):
        self.connection_uri = connection_uri
        self.client = None
        self.db = None
        
    async def connect(self):
        """Initialize MongoDB connection"""
        try:
            self.client = AsyncMongoClient(self.connection_uri)
            db_name = getattr(self, 'db_name_override', DATABASE_NAME)
            self.db = self.client[db_name]
            
            # Create indexes for better performance
            await self._create_indexes()
            
            logger.info("✅ MongoDB Atlas connected successfully")
            return True
        except Exception as e:
            logger.error(f"💥 MongoDB connection failed: {e}")
            return False
    
    async def _create_indexes(self):
        """Create database indexes"""
        content_collection = self.db[COLLECTIONS["content"]]
        
        # Timeline indexes
        await content_collection.create_index([("message_date", -1)])
        await content_collection.create_index([("channel", 1), ("message_date", -1)])
        await content_collection.create_index([("processed_at", -1)])
        
        # Sent status index for bot message tracking
        await content_collection.create_index([("sent_to_channel", 1), ("message_date", 1)])
        
        # Compound index for efficient duplicate checking
        await content_collection.create_index([
            ("content_hash", 1), 
            ("sent_to_channel", 1), 
            ("message_date", -1)
        ], background=True)
        
        # Index for unsent content queries
        await content_collection.create_index([
            ("sent_to_channel", 1), 
            ("message_date", -1)
        ], background=True)
        
        # Text search index
        await content_collection.create_index([("paraphrased_text", "text")])
        
        # Metadata indexes
        metadata_collection = self.db[COLLECTIONS["metadata"]]
        await metadata_collection.create_index([("key", 1)], unique=True)
        
        # Cache TTL index
        cache_collection = self.db[COLLECTIONS["cache"]]
        await cache_collection.create_index([("created_at", 1)], expireAfterSeconds=3600)
    
    async def get_last_scrape_timestamp(self) -> Optional[datetime]:
        """Get last scrape timestamp from MongoDB"""
        try:
            metadata_collection = self.db[COLLECTIONS["metadata"]]
            result = await metadata_collection.find_one({"key": "last_scrape_timestamp"})
            
            if result and "timestamp" in result:
                return datetime.fromtimestamp(result["timestamp"], timezone.utc)
        except Exception:
            pass
        return None
    
    async def set_last_scrape_timestamp(self, timestamp: datetime):
        """Set last scrape timestamp in MongoDB"""
        try:
            metadata_collection = self.db[COLLECTIONS["metadata"]]
            await metadata_collection.update_one(
                {"key": "last_scrape_timestamp"},
                {"$set": {
                    "key": "last_scrape_timestamp",
                    "timestamp": timestamp.timestamp(),
                    "updated_at": datetime.now(timezone.utc)
                }},
                upsert=True
            )
        except Exception:
            pass
    
    async def has_content(self) -> bool:
        """Check if database has any content"""
        try:
            content_collection = self.db[COLLECTIONS["content"]]
            count = await content_collection.count_documents({})
            return count > 0
        except Exception:
            return False
    
    async def get_content_count(self) -> int:
        """Get total content count"""
        try:
            content_collection = self.db[COLLECTIONS["content"]]
            return await content_collection.count_documents({})
        except Exception:
            return 0
    
    async def batch_store_processed_content(self, content_items: List[Tuple[str, int, Dict, datetime]]) -> int:
        """Batch store content items with ATOMIC duplicate prevention"""
        if not content_items:
            return 0
        
        try:
            content_collection = self.db[COLLECTIONS["content"]]
            operations = []
            processed_hashes = set()
            
            for channel, message_id, content_data, message_date in content_items:
                if not self._fast_validate_content(content_data):
                    continue
                
                paraphrased_text = content_data.get("paraphrased_text", "")
                content_hash = self._generate_consistent_hash(paraphrased_text)
                
                if content_hash in processed_hashes:
                    logger.info(f"⏭️ Duplicate in current batch: {paraphrased_text[:50]}...")
                    continue
                
                processed_hashes.add(content_hash)
                
                document = {
                    "_id": f"{int(message_date.timestamp())}_{channel}_{message_id}",
                    "channel": channel,
                    "message_id": str(message_id),
                    "original_text": content_data.get("original_text", ""),
                    "paraphrased_text": paraphrased_text,
                    "content_hash": content_hash,
                    "processed_at": content_data.get("processed_at", ""),
                    "message_date": message_date,
                    "processor": content_data.get("processor", "unknown"),
                    "model_used": content_data.get("model_used", ""),
                    "duplicate_decision": content_data.get("duplicate_decision", ""),
                    "created_at": datetime.now(timezone.utc),
                    "sent_to_channel": False
                }
                
                operations.append(UpdateOne(
                    {"content_hash": content_hash},
                    {
                        "$setOnInsert": document,
                        "$set": {
                            "last_seen": datetime.now(timezone.utc)
                        }
                    },
                    upsert=True
                ))
            
            if operations:
                result = await content_collection.bulk_write(operations, ordered=False)
                inserted_count = result.upserted_count
                logger.info(f"✅ Stored {inserted_count} new items, skipped {len(operations) - inserted_count} duplicates")
                return inserted_count
            
        except Exception as e:
            logger.error(f"💥 MongoDB batch store error: {e}")
        
        return 0
    
    def _generate_consistent_hash(self, text: str) -> str:
        """Generate consistent hash for duplicate detection"""
        if not text or not isinstance(text, str):
            return hashlib.md5(b"empty").hexdigest()
        
        normalized = PATTERNS.whitespace_pattern.sub(' ', text.lower().strip())
        normalized = re.sub(r'[^\w\s]', '', normalized)
        normalized = ' '.join(normalized.split())
        
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()
    
    async def get_content_since_timestamp(self, since_timestamp: datetime, limit: int = 1000) -> List[Dict]:
        """Get content items since timestamp"""
        try:
            content_collection = self.db[COLLECTIONS["content"]]
            cursor = content_collection.find(
                {"message_date": {"$gte": since_timestamp}}
            ).sort("message_date", -1).limit(limit)
            
            return await cursor.to_list(length=limit)
        except Exception:
            return []
    
    async def get_content_after_timestamp(self, after_timestamp: datetime, limit: int = 1000) -> List[Dict]:
        """Get content items after timestamp (exclusive)"""
        try:
            content_collection = self.db[COLLECTIONS["content"]]
            cursor = content_collection.find(
                {"message_date": {"$gt": after_timestamp}}
            ).sort("message_date", -1).limit(limit)
            
            return await cursor.to_list(length=limit)
        except Exception:
            return []
    
    async def get_recent_content(self, limit: int = 50) -> List[Dict]:
        """Get recent content items"""
        try:
            content_collection = self.db[COLLECTIONS["content"]]
            cursor = content_collection.find({}).sort("message_date", -1).limit(limit)
            return await cursor.to_list(length=limit)
        except Exception:
            return []
    
    async def get_recent_news(self, hours: int = 24, limit: int = 50) -> List[Dict]:
        """Get recent news from last specified hours"""
        since_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        return await self.get_content_since_timestamp(since_time, limit)
    
    def _fast_validate_content(self, content_data: Dict[str, Any]) -> bool:
        """Validate content data"""
        paraphrased_text = content_data.get("paraphrased_text")
        if not paraphrased_text or not isinstance(paraphrased_text, str):
            return False
        
        safe_text = optimized_safe_message_for_telegram(paraphrased_text)
        if safe_text.startswith("💥 Error:"):
            return False
        
        original_text = content_data.get("original_text")
        if not original_text or not isinstance(original_text, str):
            return False
        
        return True
    
    async def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
    


# Telegram bot integration

class OptimizedTelegramBot:
    """Telegram bot with MongoDB integration and advanced flood control"""
    
    def __init__(self, bot_token: str, channel_username: str, mongo_manager: OptimizedMongoManager):
        self.bot = Bot(token=bot_token)
        self.channel_username = channel_username
        self.mongo_manager = mongo_manager
        
        # Rolling window rate limiting with timestamps
        self.send_timestamps = deque(maxlen=BOT_MAX_MESSAGES_PER_MINUTE)
        
        # Adaptive flood control tracking
        self.consecutive_flood_errors = 0
        self.last_flood_wait = 0
        self.is_flood_waiting = False
        
        # Pre-compile markdown escape patterns for performance
        self.markdown_escape_table = str.maketrans({
            char: f'\\{char}' for char in PATTERNS.markdown_chars
        })
        
        logger.info(f"✅ Optimized Telegram Bot initialized for {channel_username}")
    
    async def verify_bot_permissions(self) -> bool:
        """Verify bot can send messages to channel"""
        try:
            bot_info = await self.bot.get_me()
            await self.bot.get_chat(self.channel_username)
            logger.info(f"✅ Bot verified: @{bot_info.username}")
            return True
        except Exception as e:
            logger.error(f"💥 Bot verification failed: {e}")
            return False
    
    async def send_batch_content_to_channel(self) -> int:
        """
        Send batch content to Telegram channel with improved rate limiting
        
        Features:
        - Rolling window rate limiting
        - Adaptive delays based on rate limits
        - Proper flood control handling
        """
        try:
            unsent_content = await self._get_unsent_content_fast()
            
            if not unsent_content:
                return 0
            
            logger.info(f"📤 Starting batch send of {len(unsent_content)} messages")
            sent_count = 0
            content_collection = self.mongo_manager.db[COLLECTIONS["content"]]
            
            for idx, content in enumerate(unsent_content, 1):
                # Check rate limits with rolling window
                can_send, wait_time = await self._check_rate_limits_fast()
                
                if not can_send:
                    logger.warning(f"⏸️ Rate limit reached. Waiting {wait_time:.1f}s...")
                    await asyncio.sleep(wait_time)
                
                # If we're in flood waiting state, add extra delay
                if self.is_flood_waiting and self.last_flood_wait > 0:
                    logger.info(f"⏳ Post-flood recovery delay: {self.last_flood_wait}s")
                    await asyncio.sleep(self.last_flood_wait)
                    self.is_flood_waiting = False
                    self.last_flood_wait = 0
                
                # Fast message formatting and sending
                message = self._format_message_fast(content)
                
                if not message.startswith("💥 Error:"):
                    logger.info(f"📨 Sending message {idx}/{len(unsent_content)}")
                    
                    if await self._send_message_optimized(message):
                        sent_count += 1
                        # Mark message as sent in MongoDB
                        try:
                            await content_collection.update_one(
                                {"_id": content["_id"]},
                                {"$set": {"sent_to_channel": True, "sent_at": datetime.now(timezone.utc)}}
                            )
                            logger.info(f"✅ Message {idx} sent successfully")
                        except Exception as e:
                            logger.error(f"💥 Failed to mark message as sent: {e}")
                    else:
                        logger.warning(f"⚠️ Failed to send message {idx}")
                
                # Base delay between messages (respects Telegram's limits)
                # Adjusted based on consecutive errors
                base_delay = 3.0 if self.consecutive_flood_errors > 0 else 1.5
                await asyncio.sleep(base_delay)
            
            logger.info(f"✅ Batch send complete: {sent_count}/{len(unsent_content)} messages sent")
            return sent_count
            
        except Exception as e:
            logger.error(f"💥 Batch send error: {e}")
            return 0
    
    async def _get_unsent_content_fast(self) -> List[Dict]:
        """Get unsent content with AGGRESSIVE duplicate filtering"""
        try:
            content_collection = self.mongo_manager.db[COLLECTIONS["content"]]
            
            query = {
                "sent_to_channel": {"$ne": True},
                "message_date": {"$gte": datetime.now(timezone.utc) - timedelta(hours=INITIAL_SCRAPE_HOURS)},
                "paraphrased_text": {"$not": {"$regex": "^\\[DUPLICATE\\]"}}
            }
            
            pipeline = [
                {"$match": query},
                {"$sort": {"message_date": 1}},
                {
                    "$group": {
                        "_id": "$content_hash",
                        "doc": {"$first": "$$ROOT"},
                        "count": {"$sum": 1}
                    }
                },
                {"$replaceRoot": {"newRoot": "$doc"}},
                {"$limit": 50}
            ]
            
            unique_content = await content_collection.aggregate(pipeline).to_list(length=50)
            
            validated_content = []
            for c in unique_content:
                paraphrased_text = c.get("paraphrased_text", "")
                
                if (paraphrased_text 
                    and not paraphrased_text.startswith("[DUPLICATE]")
                    and not paraphrased_text.startswith("💥 Error:")
                    and len(paraphrased_text.strip()) > 10):
                    
                    validated_content.append(c)
            
            logger.info(f"✅ Retrieved {len(validated_content)} unique unsent messages")
            return validated_content
            
        except Exception as e:
            logger.error(f"💥 Error getting unsent content: {e}")
            return []
    
    def _format_message_fast(self, content_data: Dict) -> str:
        """Format message for Telegram"""
        try:
            text = content_data.get("paraphrased_text", "")
            safe_text = optimized_safe_message_for_telegram(text)
            
            if safe_text.startswith("💥 Error:"):
                return safe_text
            
            # Fast markdown escaping using translation table
            escaped_text = safe_text.translate(self.markdown_escape_table)
            
            return escaped_text if len(escaped_text.strip()) >= 10 else "💥 Error: Message too short"
            
        except Exception as e:
            return f"💥 Error: Message formatting failed - {str(e)}"
    
    async def _check_rate_limits_fast(self) -> Tuple[bool, Optional[float]]:
        """
        Rolling window rate limit checking
        Returns: (can_send, wait_time_seconds)
        """
        now = time.monotonic()
        
        # Remove timestamps older than 60 seconds (rolling window)
        while self.send_timestamps and (now - self.send_timestamps[0]) > 60:
            self.send_timestamps.popleft()
        
        # Check if we're at the limit
        if len(self.send_timestamps) >= BOT_MAX_MESSAGES_PER_MINUTE:
            # Calculate how long to wait until the oldest message expires
            oldest_timestamp = self.send_timestamps[0]
            wait_time = 60 - (now - oldest_timestamp)
            return False, max(wait_time, 1.0)  # At least 1 second
        
        return True, None
    
    async def _parse_retry_after(self, error_message: str) -> Optional[int]:
        """Parse retry_after seconds from Telegram error message"""
        # Try different patterns for retry_after
        patterns = [
            r'retry[_ ]?after[:\s]+(\d+)',
            r'retry in (\d+) seconds?',
            r'wait (\d+) seconds?',
            r'Too Many Requests: retry after (\d+)',
        ]
        
        error_str = str(error_message).lower()
        for pattern in patterns:
            match = re.search(pattern, error_str, re.IGNORECASE)
            if match:
                return int(match.group(1))
        
        return None
    
    async def _send_message_optimized(self, message: str, max_retries: int = 3) -> bool:
        """
        Send message to Telegram with adaptive flood control
        
        Features:
        - Parses and honors Telegram's retry_after
        - Implements exponential back-off
        - Tracks consecutive flood errors
        - Updates send timestamps for rolling window
        """
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                await self.bot.send_message(
                    chat_id=self.channel_username,
                    text=message,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                
                # Success - update tracking
                self.send_timestamps.append(time.monotonic())
                self.consecutive_flood_errors = 0
                self.is_flood_waiting = False
                return True
                
            except TelegramError as e:
                error_str = str(e)
                
                # Handle flood control errors specifically
                if "flood" in error_str.lower() or "too many requests" in error_str.lower() or "429" in error_str:
                    self.consecutive_flood_errors += 1
                    self.is_flood_waiting = True
                    
                    # Try to parse retry_after from error
                    retry_after = await self._parse_retry_after(error_str)
                    
                    if retry_after:
                        wait_time = retry_after
                        logger.warning(f"⏳ Flood control: Telegram requested {wait_time}s wait")
                    else:
                        # Exponential back-off if no retry_after provided
                        wait_time = min(30 * (2 ** self.consecutive_flood_errors), 300)  # Max 5 minutes
                        logger.warning(f"⏳ Flood control: Using exponential back-off {wait_time}s (attempt {self.consecutive_flood_errors})")
                    
                    self.last_flood_wait = wait_time
                    
                    # Wait the specified time
                    logger.info(f"💤 Sleeping {wait_time}s before retry...")
                    await asyncio.sleep(wait_time)
                    
                    retry_count += 1
                    continue
                
                # Handle network errors with shorter retry
                elif isinstance(e, (TimedOut, NetworkError)):
                    retry_count += 1
                    wait_time = 2 ** retry_count  # 2, 4, 8 seconds
                    logger.warning(f"🌐 Network error: {e}. Retrying in {wait_time}s... (attempt {retry_count}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                
                # Other errors - don't retry
                else:
                    logger.error(f"💥 Send failed (non-retryable): {e}")
                    return False
            
            except Exception as e:
                # Unexpected error
                logger.error(f"💥 Unexpected send error: {e}")
                return False
        
        # Max retries reached
        logger.error(f"💥 Failed to send after {max_retries} retries")
        return False
    


# ==================== INTELLIGENT SCRAPING DECISION CACHE ====================
class ScrapingDecisionCache:
    """Lightweight cache for scraping decisions"""
    
    def __init__(self, ttl_seconds: int = 300):  # 5 minute cache
        self.ttl = ttl_seconds
        self.cache = {}
        self.last_check_time = None
        self.last_db_state = None
    
    def should_check_database(self) -> bool:
        """Check if we need to query database"""
        now = time.time()
        if self.last_check_time is None:
            return True
        return (now - self.last_check_time) >= self.ttl
    
    def cache_decision(self, should_scrape: bool, next_check_time: datetime, reason: str):
        """Cache scraping decision"""
        self.cache = {
            "should_scrape": should_scrape,
            "next_check_time": next_check_time,
            "reason": reason,
            "cached_at": time.time()
        }
        self.last_check_time = time.time()
    
    def get_cached_decision(self) -> Optional[Dict]:
        """Get cached decision if valid"""
        if not self.cache:
            return None
        
        cached_at = self.cache.get("cached_at", 0)
        if (time.time() - cached_at) < self.ttl:
            return self.cache
        
        return None

# Telegram scraper

class OptimizedTelegramScraper:
    """Telegram scraper with batch processing"""
    
    def __init__(self, batch_processor: BatchContentProcessor, mongo_manager: OptimizedMongoManager, session_str: str = None):
        self.processor = batch_processor
        self.mongo_manager = mongo_manager
        self.pending_messages = []
        self.session_str = session_str

        # Set up duplicate cache for processor
        self.duplicate_cache = SmartMongoCache(mongo_manager)
        self.processor.duplicate_cache = self.duplicate_cache

        # Initialize Telegram client later in connect() method
        self.client = None
        self.channels = {}
        logger.info("✅ Telegram Scraper initialized")
    
    async def connect(self):
        """Connect to Telegram and channels"""
        try:
            # Initialize Telegram client within async context
            if self.session_str:
                self.client = TelegramClient(StringSession(self.session_str), TELEGRAM_API_ID, TELEGRAM_API_HASH)
            else:
                self.client = TelegramClient(StringSession(), TELEGRAM_API_ID, TELEGRAM_API_HASH)

            await self.client.start()
            logger.info("✅ Telegram authentication successful")
            
            # Save session if new
            if not os.path.exists(SESSION_FILE):
                with open(SESSION_FILE, "w") as f:
                    f.write(self.client.session.save())
                logger.info("🔗 Session saved")
            
            # Connect to channels with error handling
            connected_channels = 0
            for username in CHANNEL_USERNAMES:
                try:
                    entity = await self.client.get_entity(username)
                    self.channels[username] = entity
                    connected_channels += 1
                    logger.info(f"✅ Connected to @{username}")
                except Exception as e:
                    logger.error(f"💥 Failed to connect to @{username}: {e}")
            
            return connected_channels > 0
            
        except Exception as e:
            logger.error(f"💥 Telegram connection failed: {e}")
            return False
    
    async def determine_scrape_strategy(self) -> Tuple[str, Optional[datetime], int, bool]:
        """Enhanced strategy with intelligent interval logic"""
        has_content = await self.mongo_manager.has_content()
        last_scrape_timestamp = await self.mongo_manager.get_last_scrape_timestamp()
        
        now = datetime.now(timezone.utc)
        
        # Empty database - initial scrape
        if not has_content:
            since_time = now - timedelta(hours=INITIAL_SCRAPE_HOURS)
            return "initial_scrape", since_time, INITIAL_SCRAPE_HOURS, True
        
        # Has content - check timestamp
        if last_scrape_timestamp:
            time_diff = now - last_scrape_timestamp
            minutes_since = time_diff.total_seconds() / 60
            
            # Within interval - no scrape needed
            if minutes_since < REAL_TIME_INTERVAL_MINUTES:
                next_scrape_time = last_scrape_timestamp + timedelta(minutes=REAL_TIME_INTERVAL_MINUTES)
                wait_minutes = (next_scrape_time - now).total_seconds() / 60
                return "within_interval", last_scrape_timestamp, wait_minutes, False
            
            # Past interval - real-time scrape
            return "real_time_scrape", last_scrape_timestamp, minutes_since / 60, True
        
        # No timestamp - fallback
        since_time = now - timedelta(hours=3)
        return "fallback_scrape", since_time, 3, True
    
    async def should_scrape_now(self) -> Tuple[bool, str, Optional[datetime]]:
        """Intelligent scraping decision with caching"""
        
        # Check cache first
        if hasattr(self, 'decision_cache'):
            cached = self.decision_cache.get_cached_decision()
            if cached:
                return cached["should_scrape"], cached["reason"], cached.get("next_check_time")
        
        # Get strategy
        strategy, since_time, duration, should_scrape = await self.determine_scrape_strategy()
        
        # Calculate next check time
        now = datetime.now(timezone.utc)
        if strategy == "within_interval":
            next_check = since_time + timedelta(minutes=REAL_TIME_INTERVAL_MINUTES)
            reason = f"Within interval - next scrape in {duration:.1f} minutes"
        elif should_scrape:
            next_check = now + timedelta(minutes=REAL_TIME_INTERVAL_MINUTES)
            reason = f"Scraping required - {strategy} ({duration:.1f}h span)"
        else:
            next_check = now + timedelta(minutes=5)  # Check again in 5 minutes
            reason = f"No scraping needed - {strategy}"
        
        # Cache decision
        if hasattr(self, 'decision_cache'):
            self.decision_cache.cache_decision(should_scrape, next_check, reason)
        
        return should_scrape, reason, next_check
    
    async def scrape_messages_optimized(self):
        """Scrape messages from channels"""
        if not self.channels or not self.client:
            return 0
        
        strategy, since_time, expected_hours, should_scrape = await self.determine_scrape_strategy()
        logger.info(f"🔍 Strategy: {strategy} (span: {expected_hours:.1f}h)")
        
        self.pending_messages = []
        
        async with self.client:
            for channel_name, entity in self.channels.items():
                message_count = 0
                
                try:
                    async for message in self.client.iter_messages(entity, limit=1000):
                        message_utc_time = message.date.astimezone(timezone.utc)
                        
                        if since_time and message_utc_time < since_time:
                            break
                        
                        # Message filtering
                        if message.text and len(message.text.strip()) > 10:
                            self.pending_messages.append((channel_name, message))
                            message_count += 1
                
                except Exception as e:
                    logger.error(f"💥 Error scraping @{channel_name}: {e}")
                
                logger.info(f"✅ Found {message_count} messages from @{channel_name}")
        
        total_messages = len(self.pending_messages)
        logger.info(f"🔚 Total messages to process: {total_messages}")
        return total_messages
    
    async def process_all_messages_batch(self):
        """Process all messages using batch processing"""
        if not self.pending_messages:
            return 0
        
        # Get recent news for comparison
        recent_news = await self.mongo_manager.get_recent_news(hours=24, limit=MAX_COMPARISON_NEWS)
        logger.info(f"🔄 Using {len(recent_news)} recent news")
        
        start_time = datetime.now()
        
        # Extract message texts
        message_texts = [(msg.text, msg) for channel_name, msg in self.pending_messages]
        
        # Process in batches
        logger.info(f"🚀 Processing {len(message_texts)} messages...")
        processed_results = await self.processor.process_batch_concurrent(message_texts, recent_news)
        
        # Prepare content for storage
        storage_items = []
        valid_results = 0
        
        for i, result in enumerate(processed_results):
            if result is None:
                continue
            
            # Skip duplicates
            if result["paraphrased_text"].startswith("[DUPLICATE]"):
                continue
            
            try:
                channel_name, message = self.pending_messages[i]
                message_utc_time = message.date.astimezone(timezone.utc)
                storage_items.append((channel_name, message.id, result, message_utc_time))
                valid_results += 1
            except:
                continue
        
        # Store results
        stored_count = await self.mongo_manager.batch_store_processed_content(storage_items)
        
        duration = (datetime.now() - start_time).total_seconds()
        rate = stored_count / duration if duration > 0 else 0
        
        logger.info(f"✅ Processing completed: {stored_count} messages in {duration:.1f}s ({rate:.2f} msg/s)")
        return stored_count
    
    async def scrape_and_process_optimized(self):
        """Main scraping and processing method"""
        start_time = datetime.now()
        
        # Scrape messages
        message_count = await self.scrape_messages_optimized()
        
        if message_count == 0:
            await self.mongo_manager.set_last_scrape_timestamp(datetime.now(timezone.utc))
            return 0
        
        # Process messages
        processed_count = await self.process_all_messages_batch()
        
        # Update timestamp
        await self.mongo_manager.set_last_scrape_timestamp(datetime.now(timezone.utc))
        
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"🔚 Scrape completed: {processed_count} messages in {total_duration:.1f}s")
        
        return processed_count
    


# Content scheduler

class OptimizedContentScheduler:
    """Scheduler with performance monitoring"""
    
    def __init__(self, scraper: OptimizedTelegramScraper):
        self.scraper = scraper
        self.scheduler = AsyncIOScheduler()
        self.failure_count = 0
        self.performance_stats = {
            "total_runs": 0,
            "successful_runs": 0,
            "total_processing_time": 0,
            "messages_processed": 0
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=60))
    async def intelligent_scraping_task(self):
        """Intelligent scraping task that checks if scraping is needed"""
        try:
            # Check if scraping is needed
            should_scrape, reason, next_check_time = await self.scraper.should_scrape_now()
            
            if not should_scrape:
                logger.info(f"⏭️ Skipping scrape: {reason}")
                return
            
            # Proceed with scraping
            logger.info(f"🚀 Starting scrape: {reason}")
            
            task_start = datetime.now()
            
            if not await self.scraper.connect():
                raise Exception("Failed to connect to Telegram")
            
            processed_count = await self.scraper.scrape_and_process_optimized()
            
            # Update performance stats
            task_duration = (datetime.now() - task_start).total_seconds()
            self.performance_stats["total_runs"] += 1
            self.performance_stats["successful_runs"] += 1
            self.performance_stats["total_processing_time"] += task_duration
            self.performance_stats["messages_processed"] += processed_count
            
            self.failure_count = 0
            logger.info(f"✅ Intelligent scrape completed: {processed_count} messages in {task_duration:.1f}s")
            
            # Clear cache to force fresh decision on next run
            if hasattr(self.scraper, 'decision_cache'):
                self.scraper.decision_cache.cache = {}
            
        except Exception as e:
            self.failure_count += 1
            self.performance_stats["total_runs"] += 1
            logger.error(f"💥 Intelligent scraping failed: {e}")
            
            # Clear cache on failure to ensure retry logic works
            if hasattr(self.scraper, 'decision_cache'):
                self.scraper.decision_cache.cache = {}
            
            raise
    
    async def optimized_telegram_send_task(self):
        """Telegram sending task"""
        if not bot_integration:
            return
        try:
            sent_count = await bot_integration.send_batch_content_to_channel()
            if sent_count > 0:
                logger.info(f"💾 Batch send: {sent_count} messages")
        except Exception as e:
            logger.error(f"💥 Send failed: {e}")
    
    async def start_optimized_scheduler(self):
        """Dynamic scheduler based on intelligent scraping decisions"""
        
        # Initialize decision cache in scraper
        self.scraper.decision_cache = ScrapingDecisionCache(ttl_seconds=300)
        
        # Get initial scraping decision
        should_scrape, reason, next_check_time = await self.scraper.should_scrape_now()
        
        logger.info(f"🧠 Intelligent Decision: {reason}")
        
        now = datetime.now(timezone.utc)
        
        if should_scrape:
            # Schedule immediate scrape
            initial_run_time = now + timedelta(seconds=5)
            logger.info("🚀 Scheduling immediate scrape")
        else:
            # Schedule next check based on intelligent decision
            wait_seconds = max(60, (next_check_time - now).total_seconds())
            initial_run_time = now + timedelta(seconds=wait_seconds)
            logger.info(f"⏳ Next check in {wait_seconds/60:.1f} minutes")
        
        # Schedule dynamic scraping job (checks if scraping is needed)
        self.scheduler.add_job(
            self.intelligent_scraping_task,
            "date",
            run_date=initial_run_time,
            id="intelligent_initial_run"
        )
        
        # Schedule periodic intelligent checks (every 5 minutes)
        self.scheduler.add_job(
            self.intelligent_scraping_task,
            "interval",
            minutes=5,  # Check every 5 minutes instead of 30
            id="intelligent_periodic_check"
        )
        
        # Keep bot sending job as is
        self.scheduler.add_job(
            self.optimized_telegram_send_task,
            "interval",
            minutes=BOT_SEND_INTERVAL_MINUTES,
            id="optimized_telegram_send"
        )
        
        self.scheduler.start()
        logger.info("🧠 Intelligent scheduler started")
    
# System initialization

# Initialize components
mongo_manager = OptimizedMongoManager(MONGODB_URI)
batch_processor = BatchContentProcessor(GEMINI_API_KEY)

# Load session if exists
session_string = None
if os.path.exists(SESSION_FILE):
    with open(SESSION_FILE, "r") as f:
        session_string = f.read().strip()

scraper = OptimizedTelegramScraper(batch_processor, mongo_manager, session_string)
scheduler = OptimizedContentScheduler(scraper)

# Initialize Telegram bot
bot_integration = None
if BOT_TOKEN and CHANNEL_USERNAME:
    try:
        bot_integration = OptimizedTelegramBot(BOT_TOKEN, CHANNEL_USERNAME, mongo_manager)
        logger.info("✅ Bot integration initialized")
    except Exception as e:
        logger.warning(f"⚠️ Bot initialization failed: {e}")
        bot_integration = None
else:
    logger.info("ℹ️ Bot token or channel not configured - bot functionality disabled")

# Background service

async def start_background_service():
    """Start background service with MongoDB"""
    logger.info("🚀 Starting MongoDB Atlas Telegram News Scraper")
    
    try:
        # Connect to MongoDB Atlas
        mongo_connected = await mongo_manager.connect()
        if not mongo_connected:
            logger.error("💥 Failed to connect to MongoDB Atlas")
            return
        
        # Initialize cache with MongoDB manager
        duplicate_cache = SmartMongoCache(mongo_manager)
        batch_processor.duplicate_cache = duplicate_cache
        
        # Verify bot permissions
        if bot_integration:
            bot_verified = await bot_integration.verify_bot_permissions()
            if bot_verified:
                logger.info("✅ Optimized Telegram bot verified")
            else:
                logger.warning("⚠️ Bot verification failed - continuing without bot functionality")
        else:
            logger.info("ℹ️ Bot not initialized - skipping bot verification")
        
        # Start the scheduler
        await scheduler.start_optimized_scheduler()
        logger.info("✅ Background service started successfully")
        
        # Keep the service running
        while True:
            await asyncio.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down background service...")
        scheduler.scheduler.shutdown()
        await mongo_manager.close()
        logger.info("✅ Background service stopped gracefully")
    except Exception as e:
        logger.error(f"❌ Background service error: {e}")
        await mongo_manager.close()
        raise

# Main execution

if __name__ == "__main__":
    import sys
    import io
    
    # Fix Windows console encoding for emojis
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    print("=" * 80)
    print("🚀 MONGODB ATLAS TELEGRAM NEWS SCRAPER v6.0.0 🚀")
    print("=" * 80)
    print("⚡ FEATURES:")
    print("  ✅ MongoDB Atlas Database")
    print("  ✅ Async Operations")
    print("  ✅ Database Indexes")
    print("  ✅ Cache Collections")
    print("  ✅ Batch Processing")
    print("  ✅ Duplicate Detection")
    print("  ✅ Regex Patterns")
    print("  ✅ Rate Limiting")
    print("  ✅ Connection Pooling")
    print("=" * 80)
    print("📊 PERFORMANCE:")
    print("  • High-speed processing")
    print("  • Batch operations")
    print("  • Efficient caching")
    print("  • Fast database queries")
    print("=" * 80)
    print("🗄️ DATABASE:")
    print("  • Cloud-hosted MongoDB Atlas")
    print("  • Automatic scaling")
    print("  • Built-in backup")
    print("  • Advanced queries")
    print("=" * 80)
    print("🛠️ BACKGROUND TASKS:")
    print("  • Automated scraping every 30 minutes")
    print("  • Automated posting every 2 minutes")
    print("=" * 80)
    print("🎯 READY FOR PROCESSING!")
    print("=" * 80)
    
    # Start the background service
    asyncio.run(start_background_service())