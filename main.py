import asyncio
import json
import time
import sqlite3
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set, Any
import aiohttp
import re
from collections import OrderedDict
import threading

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.core import AstrBotConfig
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)
from astrbot.core.star.filter.event_message_type import EventMessageType
from astrbot.core.message.message_event_result import MessageChain
from astrbot.api.message_components import Plain


class LRUCache:
    """LRU缓存实现"""
    
    def __init__(self, capacity: int = 1000):
        self.capacity = capacity
        self.cache = OrderedDict()
        self.lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Any]:
        with self.lock:
            if key not in self.cache:
                return None
            value = self.cache.pop(key)
            self.cache[key] = value  # 移动到最近使用
            return value
    
    def set(self, key: str, value: Any) -> None:
        with self.lock:
            if key in self.cache:
                self.cache.pop(key)
            elif len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)  # 移除最久未使用
            self.cache[key] = value
    
    def delete(self, key: str) -> bool:
        with self.lock:
            if key in self.cache:
                self.cache.pop(key)
                return True
            return False
    
    def clear(self) -> None:
        with self.lock:
            self.cache.clear()


class APIRateLimiter:
    """API调用频率限制器"""
    
    def __init__(self, max_calls_per_minute: int = 60, max_calls_per_hour: int = 1000):
        self.max_per_minute = max_calls_per_minute
        self.max_per_hour = max_calls_per_hour
        self.minute_calls: List[float] = []
        self.hour_calls: List[float] = []
        self.lock = threading.RLock()
        self.cooldown_until: Optional[float] = None
    
    def can_make_call(self) -> Tuple[bool, float]:
        """检查是否可以调用API，返回(是否可以, 需要等待的秒数)"""
        with self.lock:
            now = time.time()
            
            # 检查冷却状态
            if self.cooldown_until and now < self.cooldown_until:
                return False, self.cooldown_until - now
            
            # 清理过期记录
            minute_ago = now - 60
            hour_ago = now - 3600
            
            self.minute_calls = [t for t in self.minute_calls if t > minute_ago]
            self.hour_calls = [t for t in self.hour_calls if t > hour_ago]
            
            # 检查频率限制
            if len(self.minute_calls) >= self.max_per_minute:
                oldest = self.minute_calls[0]
                return False, 60 - (now - oldest)
            
            if len(self.hour_calls) >= self.max_per_hour:
                oldest = self.hour_calls[0]
                return False, 3600 - (now - oldest)
            
            return True, 0
    
    def record_call(self, success: bool = True) -> None:
        """记录API调用"""
        with self.lock:
            now = time.time()
            
            if success:
                self.minute_calls.append(now)
                self.hour_calls.append(now)
                # 如果之前处于冷却状态，清除它
                self.cooldown_until = None
            else:
                # 失败时进入冷却状态
                self.cooldown_until = now + 30  # 30秒冷却
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self.lock:
            now = time.time()
            minute_ago = now - 60
            hour_ago = now - 3600
            
            minute_calls = [t for t in self.minute_calls if t > minute_ago]
            hour_calls = [t for t in self.hour_calls if t > hour_ago]
            
            return {
                "minute_calls": len(minute_calls),
                "hour_calls": len(hour_calls),
                "max_per_minute": self.max_per_minute,
                "max_per_hour": self.max_per_hour,
                "in_cooldown": bool(self.cooldown_until and now < self.cooldown_until),
                "cooldown_remaining": max(0, self.cooldown_until - now) if self.cooldown_until else 0
            }


class MessageContentCache:
    """消息内容缓存管理器"""
    
    def __init__(self, cache_ttl: int = 3600, max_cache_size: int = 10000):
        self.cache_ttl = cache_ttl
        self.cache: Dict[str, Tuple[Dict, float]] = {}  # key -> (result, timestamp)
        self.max_cache_size = max_cache_size
        self.lock = threading.RLock()
    
    def _generate_key(self, text: str) -> str:
        """生成缓存键"""
        # 对消息进行归一化处理
        normalized = text.strip().lower()
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()
    
    def get_cached_result(self, text: str) -> Optional[Dict]:
        """获取缓存结果"""
        with self.lock:
            key = self._generate_key(text)
            
            if key in self.cache:
                result, timestamp = self.cache[key]
                if time.time() - timestamp < self.cache_ttl:
                    # 更新为最近访问
                    self.cache[key] = (result, timestamp)
                    return result
                else:
                    # 缓存过期，删除
                    del self.cache[key]
            
            return None
    
    def set_cached_result(self, text: str, result: Dict) -> None:
        """设置缓存结果"""
        with self.lock:
            key = self._generate_key(text)
            self.cache[key] = (result, time.time())
            
            # 清理过期缓存和限制大小
            self._cleanup()
    
    def _cleanup(self) -> None:
        """清理过期缓存"""
        with self.lock:
            current_time = time.time()
            expired_keys = []
            
            for key, (_, timestamp) in self.cache.items():
                if current_time - timestamp > self.cache_ttl:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self.cache[key]
            
            # 如果仍然超过最大大小，移除最旧的条目
            if len(self.cache) > self.max_cache_size:
                # 转换为列表进行排序
                items = list(self.cache.items())
                items.sort(key=lambda x: x[1][1])  # 按时间戳排序
                
                # 删除最旧的条目直到满足大小限制
                for key, _ in items[:len(items) - self.max_cache_size]:
                    del self.cache[key]
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        with self.lock:
            return {
                "cache_size": len(self.cache),
                "cache_hits": 0,  # 需要实际记录命中率
                "cache_misses": 0,
                "cache_ttl": self.cache_ttl,
                "max_cache_size": self.max_cache_size
            }


class DatabaseConnectionPool:
    """数据库连接池"""
    
    def __init__(self, db_path: Path, max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections: List[sqlite3.Connection] = []
        self.in_use: Set[sqlite3.Connection] = set()
        self.lock = threading.RLock()
        
        # 初始化连接池
        self._initialize_pool()
    
    def _initialize_pool(self) -> None:
        """初始化连接池"""
        with self.lock:
            for _ in range(min(5, self.max_connections)):  # 初始创建5个连接
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                self.connections.append(conn)
    
    def get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        with self.lock:
            # 首先尝试复用空闲连接
            for conn in self.connections:
                if conn not in self.in_use:
                    self.in_use.add(conn)
                    return conn
            
            # 如果没有空闲连接且未达到上限，创建新连接
            if len(self.connections) < self.max_connections:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                self.connections.append(conn)
                self.in_use.add(conn)
                return conn
            
            # 所有连接都在使用中，等待
            raise Exception("数据库连接池已满，请稍后重试")
    
    def release_connection(self, conn: sqlite3.Connection) -> None:
        """释放数据库连接"""
        with self.lock:
            if conn in self.in_use:
                self.in_use.remove(conn)
    
    def close_all(self) -> None:
        """关闭所有连接"""
        with self.lock:
            for conn in self.connections:
                try:
                    conn.close()
                except Exception as e:
                    logger.error(f"关闭数据库连接失败: {e}")
            
            self.connections.clear()
            self.in_use.clear()


class RetryManager:
    """重试管理器"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 30.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
    
    async def execute_with_retry(self, func, *args, **kwargs) -> Tuple[Any, bool, int]:
        """
        执行函数并自动重试
        
        返回: (结果, 是否成功, 重试次数)
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):  # 0到max_retries次重试
            try:
                result = await func(*args, **kwargs)
                return result, True, attempt
            except Exception as e:
                last_exception = e
                
                if attempt < self.max_retries:
                    # 计算延迟时间（指数退避）
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    delay += self.base_delay * (0.1 * attempt)  # 添加抖动
                    
                    logger.warning(f"操作失败，第{attempt + 1}次重试，等待{delay:.2f}秒: {e}")
                    
                    try:
                        await asyncio.sleep(delay)
                    except asyncio.CancelledError:
                        raise
                else:
                    logger.error(f"操作在{attempt}次重试后仍失败: {e}")
        
        return None, False, self.max_retries


class SensitiveWordAPIClient:
    """敏感词API客户端"""
    
    def __init__(self, endpoint: str, rate_limiter: APIRateLimiter, 
                 cache_manager: MessageContentCache, retry_manager: RetryManager):
        self.endpoint = endpoint
        self.rate_limiter = rate_limiter
        self.cache_manager = cache_manager
        self.retry_manager = retry_manager
        self.session: Optional[aiohttp.ClientSession] = None
        self.total_calls = 0
        self.successful_calls = 0
        self.failed_calls = 0
    
    async def ensure_session(self) -> None:
        """确保会话存在"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=10, connect=5, sock_read=5)
            self.session = aiohttp.ClientSession(timeout=timeout)
    
    async def check_text(self, text: str) -> Optional[Dict]:
        """
        检查文本是否包含敏感词
        
        返回: None表示检查失败，Dict包含检查结果
        """
        # 检查缓存
        cached_result = self.cache_manager.get_cached_result(text)
        if cached_result is not None:
            logger.debug("从缓存中获取敏感词检查结果")
            return cached_result
        
        # 检查频率限制
        can_call, wait_time = self.rate_limiter.can_make_call()
        if not can_call:
            logger.warning(f"API调用频率限制，需要等待{wait_time:.2f}秒")
            
            # 如果等待时间超过阈值，直接返回None
            if wait_time > 5:
                return None
            
            # 否则等待
            try:
                await asyncio.sleep(wait_time)
            except asyncio.CancelledError:
                return None
        
        # 执行API调用
        self.total_calls += 1
        
        try:
            # 准备请求
            await self.ensure_session()
            
            # 使用重试管理器
            result, success, retries = await self.retry_manager.execute_with_retry(
                self._make_api_request, text
            )
            
            if success:
                self.successful_calls += 1
                self.rate_limiter.record_call(success=True)
                
                # 缓存成功的结果
                if result and result.get("status") == "forbidden":
                    self.cache_manager.set_cached_result(text, result)
                
                return result
            else:
                self.failed_calls += 1
                self.rate_limiter.record_call(success=False)
                return None
                
        except Exception as e:
            self.failed_calls += 1
            self.rate_limiter.record_call(success=False)
            logger.error(f"API调用异常: {e}")
            return None
    
    async def _make_api_request(self, text: str) -> Dict:
        """实际执行API请求"""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "AstrBot-Sensitive-Word-Monitor/2.0.0"
        }
        
        payload = {"text": text}
        
        async with self.session.post(self.endpoint, json=payload, headers=headers) as response:
            if response.status == 200:
                result = await response.json()
                return result
            elif response.status == 429:  # Too Many Requests
                raise Exception(f"API调用过于频繁，状态码: {response.status}")
            else:
                raise Exception(f"API请求失败，状态码: {response.status}")
    
    async def close(self) -> None:
        """关闭会话"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "total_calls": self.total_calls,
            "successful_calls": self.successful_calls,
            "failed_calls": self.failed_calls,
            "success_rate": self.successful_calls / max(self.total_calls, 1),
            "rate_limiter_stats": self.rate_limiter.get_stats(),
            "cache_stats": self.cache_manager.get_stats()
        }


@register("sensitive_word_monitor", "AstrBot", "敏感词监控插件（优化修复版）", "2.1.0")
class SensitiveWordMonitor(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.context = context
        
        # 初始化配置
        self.group_whitelist = config.get("group_whitelist", ["QQ:GroupMessage:1030157691"])
        self.admin_qq_list = config.get("admin_qq_list", ["QQ:FriendMessage:475407353"])
        self.api_endpoint = config.get("api_endpoint", "https://uapis.cn/api/v1/text/profanitycheck")
        self.group_notice_enabled = config.get("group_notice_enabled", True)
        self.notice_template = config.get("notice_template", "")
        self.admin_notice_template = config.get("admin_notice_template", "")
        self.statistics_enabled = config.get("statistics_enabled", True)
        self.cooldown_seconds = config.get("cooldown_seconds", 60)
        self.enable_auto_ban = config.get("enable_auto_ban", True)
        self.exempt_roles = config.get("exempt_roles", ["owner", "admin"])
        self.violation_log_enabled = config.get("violation_log_enabled", True)
        self.max_log_days = config.get("max_log_days", 30)
        self.enable_message_delete = config.get("enable_message_delete", True)
        self.bypass_rate_limit = config.get("bypass_rate_limit", True)
        self.enable_local_check = config.get("enable_local_check", True)
        self.debug_mode = config.get("debug_mode", False)
        
        # API调用限制配置
        api_rate_limit = config.get("api_rate_limit", {})
        self.api_max_calls_per_minute = api_rate_limit.get("max_calls_per_minute", 60)
        self.api_max_calls_per_hour = api_rate_limit.get("max_calls_per_hour", 1000)
        
        # 缓存配置
        cache_config = config.get("cache_config", {})
        self.cache_ttl = cache_config.get("cache_ttl", 3600)
        self.max_cache_size = cache_config.get("max_cache_size", 10000)
        
        # 重试配置
        retry_config = config.get("retry_config", {})
        self.max_retries = retry_config.get("max_retries", 3)
        self.retry_base_delay = retry_config.get("base_delay", 1.0)
        self.retry_max_delay = retry_config.get("max_delay", 30.0)
        
        # 自定义违禁词
        self.custom_forbidden_words = set(config.get("custom_forbidden_words", []))
        self.local_check_patterns = self._compile_local_patterns()
        
        # 禁言规则
        ban_rules = config.get("ban_rules", {})
        self.first_ban_duration = ban_rules.get("first_ban_duration", 60)
        self.second_ban_duration = ban_rules.get("second_ban_duration", 600)
        self.third_ban_duration = ban_rules.get("third_ban_duration", 86400)
        self.reset_time = ban_rules.get("reset_time", 4)
        
        # 初始化组件
        self._init_components()
        
        # 统计数据结构
        self.statistics: Dict[str, Dict] = {
            "total_checks": 0,
            "sensitive_detected": 0,
            "auto_bans": 0,
            "by_group": {},
            "by_user": {},
            "by_word": {}
        }
        
        # 冷却时间记录（使用LRU缓存）
        self.cooldown_users = LRUCache(capacity=1000)
        
        # 消息ID缓存，用于绕过限流
        self.message_cache = LRUCache(capacity=500)
        
        # 违规记录数据库
        self.db_path = Path("data/plugin_data/sensitive_word_monitor/violations.db")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 初始化数据库连接池
        self.db_pool = DatabaseConnectionPool(self.db_path)
        self.init_database()
        
        # 启动定期清理任务
        self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
        
        logger.info("=" * 60)
        logger.info(f"敏感词监控插件 v2.1.0 已加载（已修复API调用问题）")
        logger.info(f"监控群聊：{len(self.group_whitelist)}个")
        logger.info(f"管理员：{len(self.admin_qq_list)}个")
        logger.info(f"自定义违禁词：{len(self.custom_forbidden_words)}个")
        logger.info(f"API调用限制：{self.api_max_calls_per_minute}/分钟，{self.api_max_calls_per_hour}/小时")
        logger.info(f"消息缓存：TTL={self.cache_ttl}秒，最大大小={self.max_cache_size}")
        logger.info(f"重试策略：最大{self.max_retries}次，退避延迟{self.retry_base_delay}-{self.retry_max_delay}秒")
        logger.info("=" * 60)
    
    def _init_components(self) -> None:
        """初始化各个组件"""
        # API频率限制器
        self.rate_limiter = APIRateLimiter(
            max_calls_per_minute=self.api_max_calls_per_minute,
            max_calls_per_hour=self.api_max_calls_per_hour
        )
        
        # 消息缓存管理器
        self.cache_manager = MessageContentCache(
            cache_ttl=self.cache_ttl,
            max_cache_size=self.max_cache_size
        )
        
        # 重试管理器
        self.retry_manager = RetryManager(
            max_retries=self.max_retries,
            base_delay=self.retry_base_delay,
            max_delay=self.retry_max_delay
        )
        
        # API客户端
        self.api_client = SensitiveWordAPIClient(
            endpoint=self.api_endpoint,
            rate_limiter=self.rate_limiter,
            cache_manager=self.cache_manager,
            retry_manager=self.retry_manager
        )
    
    def _compile_local_patterns(self) -> List[re.Pattern]:
        """编译本地违禁词正则表达式"""
        patterns = []
        for word in self.custom_forbidden_words:
            if word:
                try:
                    pattern = re.compile(re.escape(word), re.IGNORECASE)
                    patterns.append(pattern)
                except Exception as e:
                    logger.error(f"编译违禁词正则失败 {word}: {e}")
        return patterns
    
    def local_check(self, text: str) -> Tuple[bool, List[str]]:
        """本地违禁词检测"""
        if not self.enable_local_check or not self.local_check_patterns:
            return False, []
        
        found_words = []
        for pattern in self.local_check_patterns:
            matches = pattern.findall(text)
            if matches:
                found_words.extend(matches)
        
        return bool(found_words), list(set(found_words))
    
    def init_database(self):
        """初始化违规记录数据库"""
        conn = None
        try:
            conn = self.db_pool.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS violations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    user_name TEXT,
                    violation_count INTEGER DEFAULT 1,
                    forbidden_words TEXT,
                    original_text TEXT,
                    ban_duration INTEGER,
                    last_violation_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_group_user ON violations(group_id, user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_last_date ON violations(last_violation_date)')
            
            # 添加复合索引以提高查询性能
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_group_user_date 
                ON violations(group_id, user_id, last_violation_date DESC)
            ''')
            
            conn.commit()
            
            if self.debug_mode:
                logger.debug("违规记录数据库初始化完成")
        except Exception as e:
            logger.error(f"初始化数据库失败：{e}")
            # 尝试重新创建数据库连接
            if conn:
                try:
                    conn.close()
                except:
                    pass
            raise
        finally:
            if conn:
                self.db_pool.release_connection(conn)
    
    def is_whitelist_group(self, group_id: str) -> bool:
        """检查群聊是否在白名单中"""
        group_umo = f"QQ:GroupMessage:{group_id}"
        return group_umo in self.group_whitelist
    
    def should_check_user(self, user_id: str) -> bool:
        """检查用户是否在冷却时间内"""
        if self.bypass_rate_limit:
            return True
        
        now = time.time()
        last_check = self.cooldown_users.get(user_id)
        
        if last_check is None or (now - last_check) >= self.cooldown_seconds:
            self.cooldown_users.set(user_id, now)
            return True
        return False
    
    async def get_user_role(self, event: AiocqhttpMessageEvent) -> Optional[str]:
        """获取用户在群内的角色"""
        try:
            if hasattr(event, 'is_admin') and callable(event.is_admin):
                if event.is_admin():
                    return "admin"
            
            if hasattr(event.message_obj, 'sender'):
                sender = event.message_obj.sender
                if hasattr(sender, 'role'):
                    role = getattr(sender, 'role', '')
                    if role == 'owner':
                        return "owner"
                    elif role == 'admin':
                        return "admin"
            
            return None
        except Exception as e:
            if self.debug_mode:
                logger.error(f"获取用户角色失败：{e}")
            return None
    
    def is_exempt_from_ban(self, role: Optional[str]) -> bool:
        """检查用户是否免禁言"""
        if not role:
            return False
        return role.lower() in [r.lower() for r in self.exempt_roles]
    
    async def get_violation_info(self, group_id: str, user_id: str) -> Tuple[int, str]:
        """获取用户违规信息（次数，最后违规日期）"""
        conn = None
        try:
            conn = self.db_pool.get_connection()
            cursor = conn.cursor()
            
            now = datetime.now()
            reset_datetime = datetime(now.year, now.month, now.day, self.reset_time, 0, 0)
            
            if now.hour < self.reset_time:
                reset_datetime -= timedelta(days=1)
            
            cursor.execute('''
                SELECT violation_count, last_violation_date 
                FROM violations 
                WHERE group_id = ? AND user_id = ?
                ORDER BY last_violation_date DESC 
                LIMIT 1
            ''', (group_id, user_id))
            
            result = cursor.fetchone()
            
            if result:
                violation_count, last_date_str = result
                last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
                today = now.date()
                
                if last_date < today:
                    return 1, str(today)
                else:
                    return violation_count + 1, last_date_str
            else:
                return 1, str(now.date())
                
        except Exception as e:
            if self.debug_mode:
                logger.error(f"获取违规信息失败：{e}")
            return 1, str(datetime.now().date())
        finally:
            if conn:
                self.db_pool.release_connection(conn)
    
    async def update_violation_record(self, group_id: str, user_id: str, user_name: str, 
                                     violation_count: int, forbidden_words: List[str], 
                                     original_text: str, ban_duration: int):
        """更新违规记录"""
        conn = None
        try:
            conn = self.db_pool.get_connection()
            cursor = conn.cursor()
            
            today = datetime.now().date()
            
            cursor.execute('''
                INSERT OR REPLACE INTO violations 
                (group_id, user_id, user_name, violation_count, forbidden_words, 
                 original_text, ban_duration, last_violation_date, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ''', (
                group_id, user_id, user_name, violation_count, 
                json.dumps(forbidden_words, ensure_ascii=False),
                original_text[:500],
                ban_duration,
                str(today)
            ))
            
            # 清理过期记录
            cutoff_date = (datetime.now() - timedelta(days=self.max_log_days)).date()
            cursor.execute('DELETE FROM violations WHERE last_violation_date < ?', (str(cutoff_date),))
            
            conn.commit()
            
            if self.debug_mode:
                logger.debug(f"更新违规记录：群{group_id} 用户{user_id} 第{violation_count}次违规")
            
        except Exception as e:
            logger.error(f"更新违规记录失败：{e}")
            # 尝试重新连接
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            if conn:
                self.db_pool.release_connection(conn)
    
    async def delete_message(self, event: AiocqhttpMessageEvent) -> bool:
        """撤回消息"""
        try:
            if not self.enable_message_delete:
                return False
            
            message_id = event.message_obj.message_id
            if hasattr(event.bot, 'delete_msg'):
                await event.bot.delete_msg(message_id=message_id)
                if self.debug_mode:
                    logger.debug(f"已撤回消息 {message_id}")
                return True
            else:
                logger.warning("当前平台不支持消息撤回")
                return False
        except Exception as e:
            if self.debug_mode:
                logger.error(f"撤回消息失败：{e}")
            return False
    
    async def check_sensitive_words(self, text: str) -> Optional[Dict]:
        """调用敏感词检测API（使用优化后的客户端）"""
        try:
            result = await self.api_client.check_text(text)
            return result
        except Exception as e:
            logger.error(f"敏感词检测失败：{e}")
            return None
    
    async def ban_user(self, event: AiocqhttpMessageEvent, user_id: str, duration: int) -> bool:
        """禁言用户"""
        try:
            group_id = event.get_group_id()
            
            if hasattr(event, 'bot') and hasattr(event.bot, 'set_group_ban'):
                await event.bot.set_group_ban(
                    group_id=int(group_id),
                    user_id=int(user_id),
                    duration=duration
                )
                if self.debug_mode:
                    logger.debug(f"已禁言用户 {user_id}，时长 {duration} 秒")
                return True
            else:
                logger.warning("当前平台不支持禁言操作")
                return False
                
        except Exception as e:
            logger.error(f"禁言用户失败：{e}")
            return False
    
    def update_statistics(self, group_id: str, user_id: str, forbidden_words: List[str], 
                         has_sensitive: bool, was_banned: bool = False):
        """更新统计信息"""
        if not self.statistics_enabled:
            return
        
        self.statistics["total_checks"] += 1
        
        if has_sensitive:
            self.statistics["sensitive_detected"] += 1
            
            if was_banned:
                self.statistics["auto_bans"] += 1
            
            if group_id not in self.statistics["by_group"]:
                self.statistics["by_group"][group_id] = {
                    "total": 0,
                    "bans": 0,
                    "users": set(),
                    "words": {}
                }
            self.statistics["by_group"][group_id]["total"] += 1
            if was_banned:
                self.statistics["by_group"][group_id]["bans"] += 1
            self.statistics["by_group"][group_id]["users"].add(user_id)
            
            user_key = f"{group_id}:{user_id}"
            if user_key not in self.statistics["by_user"]:
                self.statistics["by_user"][user_key] = {"total": 0, "bans": 0}
            self.statistics["by_user"][user_key]["total"] += 1
            if was_banned:
                self.statistics["by_user"][user_key]["bans"] += 1
            
            for word in forbidden_words:
                if word not in self.statistics["by_word"]:
                    self.statistics["by_word"][word] = {"total": 0, "bans": 0}
                self.statistics["by_word"][word]["total"] += 1
                if was_banned:
                    self.statistics["by_word"][word]["bans"] += 1
    
    def format_notice(self, template: str, **kwargs) -> str:
        """格式化提醒消息"""
        try:
            return template.format(**kwargs)
        except Exception as e:
            if self.debug_mode:
                logger.error(f"格式化消息失败：{e}")
            return template
    
    async def send_admin_notice(self, group_id: str, user_id: str, user_name: str, 
                               forbidden_words: List[str], original_text: str,
                               violation_count: int, ban_duration: int):
        """给所有管理员发送私聊提醒"""
        if not self.admin_qq_list:
            return
        
        for admin_umo in self.admin_qq_list:
            try:
                notice_content = self.format_notice(
                    self.admin_notice_template,
                    group_id=group_id,
                    user_name=user_name,
                    user_id=user_id,
                    forbidden_words=", ".join(forbidden_words),
                    original_text=original_text[:100] + ("..." if len(original_text) > 100 else ""),
                    violation_count=violation_count,
                    ban_duration=ban_duration,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
                
                # 添加严重性提示
                if violation_count >= 3:
                    notice_content = f"⚠️⚠️⚠️ 严重违规！第三次违规！\n" + notice_content
                
                # 发送私聊消息
                message_chain = MessageChain()
                message_chain.chain = [Plain(notice_content)]
                
                await self.context.send_message(admin_umo, message_chain)
                
                if self.debug_mode:
                    logger.debug(f"已向管理员 {admin_umo} 发送敏感词提醒（第{violation_count}次违规）")
            except Exception as e:
                logger.error(f"向管理员 {admin_umo} 发送提醒失败：{e}")
    
    async def _periodic_cleanup(self):
        """定期清理任务"""
        try:
            while True:
                await asyncio.sleep(3600)  # 每小时清理一次
                
                try:
                    # 清理过期缓存
                    self.cache_manager._cleanup()
                    
                    # 清理过期冷却记录
                    current_time = time.time()
                    keys_to_remove = []
                    
                    # 注意：这里简化处理，实际应该使用更好的数据结构
                    for user_id, last_time in self.cooldown_users.cache.items():
                        if current_time - last_time > self.cooldown_seconds * 2:  # 两倍冷却时间
                            keys_to_remove.append(user_id)
                    
                    for key in keys_to_remove:
                        self.cooldown_users.delete(key)
                    
                    if self.debug_mode:
                        logger.debug(f"定期清理完成，移除了{len(keys_to_remove)}个过期冷却记录")
                        
                except Exception as e:
                    logger.error(f"定期清理任务失败：{e}")
                    
        except asyncio.CancelledError:
            logger.info("定期清理任务已取消")
        except Exception as e:
            logger.error(f"定期清理任务异常退出：{e}")
    
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(EventMessageType.GROUP_MESSAGE)
    async def monitor_group_message(self, event: AiocqhttpMessageEvent):
        """监控群消息，检测敏感词并执行阶梯式禁言"""
        try:
            # 获取基本信息
            group_id = event.get_group_id()
            user_id = str(event.message_obj.sender.user_id)
            user_name = event.get_sender_name()
            message_text = event.message_str
            
            if self.debug_mode:
                logger.debug(f"收到消息：群{group_id} 用户{user_id} 内容：{message_text[:50]}")
            
            # 检查是否在白名单群聊中
            if not self.is_whitelist_group(group_id):
                if self.debug_mode:
                    logger.debug(f"群{group_id}不在白名单中，忽略")
                return
            
            # 检查冷却时间
            if not self.should_check_user(user_id):
                if self.debug_mode:
                    logger.debug(f"用户{user_id}在冷却时间内，忽略")
                return
            
            # 本地违禁词检测
            local_hit, local_words = self.local_check(message_text)
            if local_hit and local_words:
                logger.info(f"本地检测到敏感词：{local_words}")
                
                # 获取用户违规信息
                violation_count, violation_date = await self.get_violation_info(group_id, user_id)
                
                # 确定禁言时长
                ban_duration = 0
                if self.enable_auto_ban:
                    if violation_count == 1:
                        ban_duration = self.first_ban_duration
                    elif violation_count == 2:
                        ban_duration = self.second_ban_duration
                    else:
                        ban_duration = self.third_ban_duration
                
                # 撤回消息
                await self.delete_message(event)
                
                # 检查用户是否免禁言
                user_role = await self.get_user_role(event)
                was_banned = False
                
                if ban_duration > 0 and not self.is_exempt_from_ban(user_role):
                    ban_success = await self.ban_user(event, user_id, ban_duration)
                    was_banned = ban_success
                
                # 更新违规记录
                await self.update_violation_record(
                    group_id, user_id, user_name, violation_count, 
                    local_words, message_text, ban_duration
                )
                
                # 更新统计
                self.update_statistics(group_id, user_id, local_words, True, was_banned)
                
                # 发送群内提醒
                if self.group_notice_enabled and self.notice_template:
                    notice_content = self.format_notice(
                        self.notice_template,
                        forbidden_words=", ".join(local_words),
                        original_text=message_text[:50] + ("..." if len(message_text) > 50 else ""),
                        violation_count=violation_count,
                        ban_duration=ban_duration
                    )
                    
                    if was_banned:
                        if ban_duration >= 3600:
                            hours = ban_duration // 3600
                            notice_content += f"\n已执行禁言 {hours} 小时"
                        elif ban_duration >= 60:
                            minutes = ban_duration // 60
                            notice_content += f"\n已执行禁言 {minutes} 分钟"
                        else:
                            notice_content += f"\n已执行禁言 {ban_duration} 秒"
                    
                    yield event.plain_result(notice_content)
                
                # 发送管理员通知（每次违规都发送）
                await self.send_admin_notice(
                    group_id, user_id, user_name, 
                    local_words, message_text,
                    violation_count, ban_duration
                )
                
                logger.info(f"本地检测敏感词 - 群{group_id} 用户{user_id}: {local_words}（第{violation_count}次违规）")
                return
            
            # API敏感词检测（使用优化后的客户端）
            result = await self.check_sensitive_words(message_text)
            
            if result and result.get("status") == "forbidden":
                forbidden_words = result.get("forbidden_words", [])
                original_text = result.get("original_text", "")
                
                logger.info(f"API检测到敏感词：{forbidden_words}")
                
                # 获取用户违规信息
                violation_count, violation_date = await self.get_violation_info(group_id, user_id)
                
                # 确定禁言时长
                ban_duration = 0
                if self.enable_auto_ban:
                    if violation_count == 1:
                        ban_duration = self.first_ban_duration
                    elif violation_count == 2:
                        ban_duration = self.second_ban_duration
                    else:
                        ban_duration = self.third_ban_duration
                
                # 撤回消息
                await self.delete_message(event)
                
                # 检查用户是否免禁言
                user_role = await self.get_user_role(event)
                was_banned = False
                
                if ban_duration > 0 and not self.is_exempt_from_ban(user_role):
                    ban_success = await self.ban_user(event, user_id, ban_duration)
                    was_banned = ban_success
                
                # 更新违规记录
                await self.update_violation_record(
                    group_id, user_id, user_name, violation_count, 
                    forbidden_words, original_text, ban_duration
                )
                
                # 更新统计
                self.update_statistics(group_id, user_id, forbidden_words, True, was_banned)
                
                # 发送群内提醒
                if self.group_notice_enabled and self.notice_template:
                    notice_content = self.format_notice(
                        self.notice_template,
                        forbidden_words=", ".join(forbidden_words),
                        original_text=original_text[:50] + ("..." if len(original_text) > 50 else ""),
                        violation_count=violation_count,
                        ban_duration=ban_duration
                    )
                    
                    if was_banned:
                        if ban_duration >= 3600:
                            hours = ban_duration // 3600
                            notice_content += f"\n已执行禁言 {hours} 小时"
                        elif ban_duration >= 60:
                            minutes = ban_duration // 60
                            notice_content += f"\n已执行禁言 {minutes} 分钟"
                        else:
                            notice_content += f"\n已执行禁言 {ban_duration} 秒"
                    
                    yield event.plain_result(notice_content)
                
                # 发送管理员通知（每次违规都发送）
                await self.send_admin_notice(
                    group_id, user_id, user_name, 
                    forbidden_words, original_text,
                    violation_count, ban_duration
                )
                
                logger.info(f"API检测敏感词 - 群{group_id} 用户{user_id}: {forbidden_words}（第{violation_count}次违规）")
            elif result is None:
                # API调用失败，使用优雅降级策略
                logger.warning(f"API调用失败，使用本地检测作为降级方案")
                
                # 这里可以添加降级逻辑，比如使用更严格的本地检测
                # 暂时不做处理，等待API恢复
                pass
            else:
                # 更新统计（无敏感词）
                self.update_statistics(group_id, user_id, [], False)
                
        except Exception as e:
            logger.error(f"敏感词监控处理异常：{e}")
            if self.debug_mode:
                import traceback
                logger.error(f"详细堆栈：{traceback.format_exc()}")
    
    # 添加新的命令：API统计
    @filter.command("API统计")
    async def show_api_stats(self, event: AiocqhttpMessageEvent):
        """显示API调用统计"""
        try:
            stats = self.api_client.get_stats()
            
            message = "📊 API调用统计\n"
            message += f"总调用次数：{stats['total_calls']}\n"
            message += f"成功调用：{stats['successful_calls']}\n"
            message += f"失败调用：{stats['failed_calls']}\n"
            message += f"成功率：{stats['success_rate']*100:.1f}%\n\n"
            
            rate_stats = stats['rate_limiter_stats']
            message += "频率限制状态：\n"
            message += f"  本分钟调用：{rate_stats['minute_calls']}/{rate_stats['max_per_minute']}\n"
            message += f"  本小时调用：{rate_stats['hour_calls']}/{rate_stats['max_per_hour']}\n"
            
            if rate_stats['in_cooldown']:
                message += f"  冷却中，剩余：{rate_stats['cooldown_remaining']:.1f}秒\n"
            
            cache_stats = stats['cache_stats']
            message += f"\n缓存统计：\n"
            message += f"  缓存条目：{cache_stats['cache_size']}/{cache_stats['max_cache_size']}\n"
            message += f"  缓存TTL：{cache_stats['cache_ttl']}秒\n"
            
            yield event.plain_result(message)
            
        except Exception as e:
            logger.error(f"获取API统计失败：{e}")
            yield event.plain_result("获取统计信息失败")
    
    # 添加新的命令：重置API限制
    @filter.command("重置API限制")
    async def reset_api_limit(self, event: AiocqhttpMessageEvent):
        """重置API频率限制"""
        try:
            user_id = str(event.message_obj.sender.user_id)
            
            if not any(admin_umo.endswith(user_id) for admin_umo in self.admin_qq_list):
                yield event.plain_result("仅管理员可重置API限制")
                return
            
            # 重置频率限制器
            self.rate_limiter = APIRateLimiter(
                max_calls_per_minute=self.api_max_calls_per_minute,
                max_calls_per_hour=self.api_max_calls_per_hour
            )
            
            # 重新初始化API客户端
            self.api_client = SensitiveWordAPIClient(
                endpoint=self.api_endpoint,
                rate_limiter=self.rate_limiter,
                cache_manager=self.cache_manager,
                retry_manager=self.retry_manager
            )
            
            yield event.plain_result("✅ API频率限制已重置")
            logger.info(f"API频率限制已重置")
            
        except Exception as e:
            logger.error(f"重置API限制失败：{e}")
            yield event.plain_result("重置失败")
    
    # 添加新的命令：清理缓存
    @filter.command("清理缓存")
    async def clear_cache(self, event: AiocqhttpMessageEvent):
        """清理消息缓存"""
        try:
            user_id = str(event.message_obj.sender.user_id)
            
            if not any(admin_umo.endswith(user_id) for admin_umo in self.admin_qq_list):
                yield event.plain_result("仅管理员可清理缓存")
                return
            
            # 重新创建缓存管理器以清空缓存
            self.cache_manager = MessageContentCache(
                cache_ttl=self.cache_ttl,
                max_cache_size=self.max_cache_size
            )
            
            # 重新创建API客户端以使用新的缓存管理器
            self.api_client = SensitiveWordAPIClient(
                endpoint=self.api_endpoint,
                rate_limiter=self.rate_limiter,
                cache_manager=self.cache_manager,
                retry_manager=self.retry_manager
            )
            
            yield event.plain_result("✅ 消息缓存已清理")
            logger.info(f"消息缓存已清理")
            
        except Exception as e:
            logger.error(f"清理缓存失败：{e}")
            yield event.plain_result("清理失败")
    
    async def terminate(self):
        """插件卸载时的清理工作"""
        try:
            # 取消定期清理任务
            if hasattr(self, 'cleanup_task'):
                self.cleanup_task.cancel()
                try:
                    await self.cleanup_task
                except asyncio.CancelledError:
                    pass
            
            # 关闭API客户端会话
            if hasattr(self, 'api_client'):
                await self.api_client.close()
            
            # 关闭数据库连接池
            if hasattr(self, 'db_pool'):
                self.db_pool.close_all()
            
            logger.info("敏感词监控插件已安全卸载")
        except Exception as e:
            logger.error(f"插件卸载清理失败：{e}")
