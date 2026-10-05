from dataclasses import dataclass, field, asdict
from pathlib import Path
from zoneinfo import ZoneInfo
import os
import re
import logging


def boolean(name, default):
    value = os.getenv(name, str(default)).lower()
    if value not in ('true', 'false'):
        raise ValueError(f'{name} must be true or false')
    return value == 'true'


@dataclass
class Settings:
    database: str = field(default='runtime/square/desk.sqlite3', repr=False)
    artifacts: str = 'runtime/square/artifacts'
    timezone: str = 'Asia/Kolkata'
    paper_mode: bool = True
    mode: str = 'approval'
    ai_auto_publish: bool = False
    auto_articles: bool = False
    market_universe: str = 'watchlist'
    universe_limit: int = 100
    scan_batch_size: int = 25
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
    ai_provider_order: tuple[str, ...] = ('groq', 'cerebras', 'google', 'openrouter', 'mistral', 'cloudflare', 'kilo')
    ai_max_provider_attempts: int = 4
    cerebras_api_key: str = field(default='', repr=False)
    cerebras_model: str = 'gpt-oss-120b'
    google_api_key: str = field(default='', repr=False)
    google_model: str = 'gemma-4-31b-it'
    openrouter_api_key: str = field(default='', repr=False)
    openrouter_model: str = 'openrouter/free'
    openrouter_models: tuple[str, ...] = ()
    openrouter_max_model_attempts: int = 10
    mistral_api_key: str = field(default='', repr=False)
    mistral_model: str = 'mistral-small-latest'
    cloudflare_api_token: str = field(default='', repr=False)
    cloudflare_account_id: str = field(default='', repr=False)
    cloudflare_model: str = ''
    kilo_enabled: bool = False
    kilo_api_key: str = field(default='', repr=False)
    kilo_model: str = 'kilo-auto/free'
    nvidia_api_key: str = field(default='', repr=False)
    nvidia_model: str = ''
    cohere_api_key: str = field(default='', repr=False)
    cohere_model: str = ''
    similarity_threshold: float = .78
    fee_bps: float = 10
    slippage_bps: float = 5
    retention_days: int = 30
    snapshot_retention_days: int = 2
    neon_batch_seconds: int = 900
    report_weekday: int = 6
    report_hour: int = 20
    report_minute: int = 0
    publishing_hours: tuple[int, ...] = tuple(range(7, 24))

    @classmethod
    def from_env(cls):
        from dotenv import load_dotenv
        load_dotenv('.env.square', override=False)
        s = cls()
        # Dedicated URL takes precedence over the legacy local SQLite path.
        database_url = os.getenv('DESK_DATABASE_URL') or os.getenv('DATABASE_URL')
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
        if database_url:
            if not database_url.startswith(('postgresql://', 'postgres://')):
                raise ValueError('DATABASE_URL must be a PostgreSQL connection URL')
            s.database = database_url
        s.validate()
        return s

    def validate(self):
        ZoneInfo(self.timezone)
        if self.mode not in ('automatic', 'approval', 'hybrid'):
            raise ValueError('Invalid publication mode')
        if self.market_universe not in ('watchlist', 'top_liquid'):
            raise ValueError('Market universe must be watchlist or top_liquid')
        if not 10 <= self.universe_limit <= 200 or not 5 <= self.scan_batch_size <= 50:
            raise ValueError('Universe limit must be 10..200 and scan batch size 5..50')
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
        if not 1 <= self.snapshot_retention_days <= self.retention_days:
            raise ValueError('Snapshot retention must be 1..retention_days')
        if self.neon_batch_seconds != 0 and not 900 <= self.neon_batch_seconds <= 3600:
            raise ValueError('Neon batch interval must be 0 or 900..3600 seconds')
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
        names = {'primary', 'groq', 'cerebras', 'google', 'openrouter', 'mistral', 'cloudflare', 'kilo', 'nvidia', 'cohere'}
        if (not self.ai_provider_order or any(p not in names for p in self.ai_provider_order)
                or len(set(self.ai_provider_order)) != len(self.ai_provider_order)):
            raise ValueError('Invalid AI provider order')
        if not 1 <= self.ai_max_provider_attempts <= 10 or self.ai_daily_requests <= 0 or self.ai_daily_tokens <= 0:
            raise ValueError('AI budgets must be positive and provider attempts 1..10')
        if self.openrouter_api_key and not (self.openrouter_model == 'openrouter/free' or self.openrouter_model.endswith(':free')):
            raise ValueError('OpenRouter fallback must use a free-only model')
        if (not 1 <= self.openrouter_max_model_attempts <= 10 or len(self.openrouter_models) > 10
                or len(set(self.openrouter_models)) != len(self.openrouter_models)
                or any(not m.endswith(':free') or len(m) > 160 or not re.fullmatch(r'[A-Za-z0-9_./:-]+', m)
                       for m in self.openrouter_models)):
            raise ValueError('OpenRouter model chain requires at most ten distinct free-only model IDs')
        if self.kilo_enabled and not (self.kilo_model == 'kilo-auto/free' or self.kilo_model.endswith(':free')):
            raise ValueError('Kilo fallback must use a free-only model')
        if self.google_api_key and self.google_model not in ('gemma-4-31b-it', 'gemma-4-26b-a4b-it', 'gemini-3-flash-preview'):
            raise ValueError('Google model must be a supported hosted Gemma 4 or Gemini 3 Flash model')
        for name in ('cerebras_model', 'google_model', 'openrouter_model', 'mistral_model', 'cloudflare_model', 'kilo_model', 'nvidia_model', 'cohere_model'):
            model = getattr(self, name)
            if len(model) > 160 or any(ord(c) < 32 for c in model):
                raise ValueError('Invalid AI model ID')
        if self.cloudflare_account_id and not re.fullmatch(r'[a-fA-F0-9]{32}', self.cloudflare_account_id):
            raise ValueError('Cloudflare account ID must be 32 hexadecimal characters')
        if len(self.news_feeds) > 10 or any(not url.startswith('https://') for url in self.news_feeds):
            raise ValueError('Use at most ten HTTPS official RSS feeds')
        if self.telegram_token and (not self.telegram_admins or not self.webhook_secret):
            raise ValueError('Telegram requires numeric administrator IDs and a webhook secret')
        if self.live_enabled and (not self.square_key or not self.admin_token or not self.policy_reviewed):
            raise ValueError('Live adapter requires Square key, admin token, and policy review date')
        self.prepare_storage()

    def prepare_storage(self):
        """Handle a missing Render disk without creating a privileged directory.

        Only the conventional /var/data paths may fall back. An existing disk
        with permission problems must fail, rather than silently switching to
        an empty database and losing publication history.
        """
        prefix = '/var/data'
        postgres = self.database.startswith(('postgresql://', 'postgres://'))
        if postgres:
            from urllib.parse import urlsplit, parse_qs
            try:
                url = urlsplit(self.database)
                secure = parse_qs(url.query).get('sslmode', [''])[0]
                if not url.hostname or not url.path.strip('/') or secure not in ('require', 'verify-ca', 'verify-full'):
                    raise ValueError
            except ValueError:
                raise ValueError('PostgreSQL URL requires a host, database and sslmode=require or stronger') from None
        disk_paths = {
            name: value for name, value in (('database', self.database), ('artifacts', self.artifacts))
            if value == prefix or value.startswith(prefix + '/')
        }
        if disk_paths and not Path(prefix).exists():
            fallback = Path(__file__).resolve().parents[1] / 'runtime' / 'square'
            for name, value in disk_paths.items():
                relative = value[len(prefix):].lstrip('/')
                # Both locations remain inside the application runtime folder.
                destination = (fallback / relative).resolve()
                if not destination.is_relative_to(fallback.resolve()):
                    raise ValueError('Storage path escapes the runtime directory')
                setattr(self, name, str(destination))
            if not postgres:
                self.live_enabled = False
            logging.getLogger('square_desk').warning(
                'Persistent disk /var/data is missing. Using application-local '
                'artifact storage; files may be lost on redeploy/restart. ' +
                ('PostgreSQL keeps database state durable.' if postgres else
                 'Database data may be lost; live Square publishing is disabled until durable storage is configured.')
            )
        Path(self.artifacts).mkdir(parents=True, exist_ok=True)
        if not postgres and self.database != ':memory:':
            Path(self.database).parent.mkdir(parents=True, exist_ok=True)

    def public(self):
        excluded = {'database', 'admin_token', 'telegram_token', 'webhook_secret', 'square_key', 'ai_key', 'ai_url', 'news_feeds'}
        excluded.update(name for name, value in self.__dataclass_fields__.items() if not value.repr)
        return {**{k: v for k, v in asdict(self).items() if k not in excluded},
                'database_backend': 'postgresql' if self.database.startswith(('postgresql://', 'postgres://')) else 'sqlite',
                'news_feed_count': len(self.news_feeds)}
