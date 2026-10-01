from dataclasses import dataclass, field, asdict
from pathlib import Path
from zoneinfo import ZoneInfo
import os
import re


def boolean(name, default):
    value = os.getenv(name, str(default)).lower()
    if value not in ('true', 'false'):
        raise ValueError(f'{name} must be true or false')
    return value == 'true'


@dataclass
class Settings:
    database: str = 'runtime/square/desk.sqlite3'
    artifacts: str = 'runtime/square/artifacts'
    timezone: str = 'Asia/Kolkata'
    paper_mode: bool = True
    mode: str = 'approval'
    admin_token: str = field(default='', repr=False)
    telegram_token: str = field(default='', repr=False)
    telegram_admins: tuple[int, ...] = ()
    webhook_secret: str = field(default='', repr=False)
    telegram_polling: bool = False
    square_key: str = field(default='', repr=False)
    live_enabled: bool = False
    policy_reviewed: str = ''
    policy_max_age_days: int = 30
    daily_target: int = 12
    platform_daily_cap: int = 100
    hourly_cap: int = 2
    gap_seconds: int = 1800
    scan_seconds: int = 300
    symbols: tuple[str, ...] = ('BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'LINK', 'AVAX', 'ADA')
    providers: tuple[str, ...] = ('binance', 'coinbase')
    enable_derivatives: bool = False
    news_feeds: tuple[str, ...] = ()
    candle_max_age: int = 1200
    draft_max_age: int = 1800
    min_confidence: float = 75
    min_relative_volume: float = 1.5
    min_quote_volume: float = 100000
    shock_percent: float = 3
    max_drafts_per_scan: int = 3
    post_min_words: int = 100
    post_max_words: int = 200
    article_min_words: int = 1000
    article_max_words: int = 2000
    image_daily_cap: int = 25
    article_daily_cap: int = 2
    campaign_daily_cap: int = 5
    ai_url: str = ''
    ai_key: str = field(default='', repr=False)
    ai_model: str = ''
    ai_daily_requests: int = 30
    ai_daily_tokens: int = 40000
    similarity_threshold: float = .78
    fee_bps: float = 10
    slippage_bps: float = 5
    retention_days: int = 30
    report_weekday: int = 6
    report_hour: int = 20
    report_minute: int = 0
    publishing_hours: tuple[int, ...] = tuple(range(7, 24))

    @classmethod
    def from_env(cls):
        from dotenv import load_dotenv
        load_dotenv('.env.square', override=False)
        s = cls()
        for name in s.__dataclass_fields__:
            env = os.getenv('DESK_' + name.upper())
            if env is None or env == '':
                continue
            default = getattr(s, name)
            if isinstance(default, bool):
                value = boolean('DESK_' + name.upper(), default)
            elif isinstance(default, tuple):
                value = tuple(x.strip() for x in env.split(',') if x.strip())
                if name in ('telegram_admins', 'publishing_hours'):
                    value = tuple(map(int, value))
            elif isinstance(default, int):
                value = int(env)
            elif isinstance(default, float):
                value = float(env)
            else:
                value = env
            setattr(s, name, value)
        s.validate()
        return s

    def validate(self):
        ZoneInfo(self.timezone)
        if self.mode not in ('automatic', 'approval', 'hybrid'):
            raise ValueError('Invalid publication mode')
        if not self.providers or any(x not in ('binance', 'coinbase') for x in self.providers):
            raise ValueError('Supported market providers: binance,coinbase')
        if not 1 <= len(self.symbols) <= 30 or any(not re.fullmatch(r'[A-Z0-9]{2,12}', x) for x in self.symbols):
            raise ValueError('Use 1–30 uppercase asset symbols')
        for name in ('daily_target', 'platform_daily_cap', 'hourly_cap', 'gap_seconds', 'scan_seconds',
                     'candle_max_age', 'draft_max_age', 'max_drafts_per_scan', 'retention_days'):
            if getattr(self, name) <= 0:
                raise ValueError(f'{name} must be positive')
        if self.platform_daily_cap > 100 or self.scan_seconds < 60:
            raise ValueError('Daily cap must be <=100 and scan interval >=60 seconds')
        if not 0 <= self.min_confidence <= 100 or not 0 < self.similarity_threshold <= 1:
            raise ValueError('Invalid confidence or similarity threshold')
        if self.fee_bps < 0 or self.slippage_bps < 0 or self.policy_max_age_days <= 0:
            raise ValueError('Costs must be nonnegative and policy lifetime positive')
        if self.post_min_words > self.post_max_words or self.article_min_words > self.article_max_words:
            raise ValueError('Invalid word limits')
        if not self.publishing_hours or any(not 0 <= h <= 23 for h in self.publishing_hours):
            raise ValueError('Publishing hours must be within 0–23')
        if not 0 <= self.report_weekday <= 6 or not 0 <= self.report_hour <= 23 or not 0 <= self.report_minute <= 59:
            raise ValueError('Invalid weekly report time')
        if self.ai_url and not self.ai_url.startswith('https://'):
            raise ValueError('AI endpoint must use HTTPS')
        if len(self.news_feeds) > 10 or any(not url.startswith('https://') for url in self.news_feeds):
            raise ValueError('Use at most ten HTTPS official RSS feeds')
        if self.telegram_token and (not self.telegram_admins or not self.webhook_secret):
            raise ValueError('Telegram requires numeric administrator IDs and a webhook secret')
        if self.live_enabled and (not self.square_key or not self.admin_token or not self.policy_reviewed):
            raise ValueError('Live adapter requires Square key, admin token, and policy review date')
        Path(self.artifacts).mkdir(parents=True, exist_ok=True)

    def public(self):
        excluded = {'admin_token', 'telegram_token', 'webhook_secret', 'square_key', 'ai_key', 'ai_url', 'news_feeds'}
        return {**{k: v for k, v in asdict(self).items() if k not in excluded}, 'news_feed_count': len(self.news_feeds)}
