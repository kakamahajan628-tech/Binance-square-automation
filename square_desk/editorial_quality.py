"""Bounded editorial checks, not a prediction model or platform approval claim."""
import re

REVISION = '2026-11-10-editorial-quality-33'
TOKEN = re.compile(r'\{\{([A-Za-z0-9_]+)\}\}')
NUMBER = r'[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?'
LABEL = re.compile(
    r'\b(?P<label>relative volume|volume ratio|RSI|MACD|support|resistance|'
    r'daily (?:price )?(?:change|return)|hourly (?:price )?(?:change|return))'
    r'\s*(?:(?:is|of|at|reads|sits at|stands at)\s+|[:=]\s*)?'
    r'(?P<value>\{\{[A-Za-z0-9_]+\}\}|' + NUMBER + r')(?!\w|\.\d)', re.I)

INSTRUCTION = (
    ' Editorial accuracy: give one concrete observation, explain one useful mechanism, '
    'and state what future closed-candle evidence could weaken the interpretation. '
    'Current measurements describe only the saved observation; never assign them to a future candle. '
    'Use relative volume {{relative_volume_display}}x baseline, not absolute volume, when available. '
    'Relative volume is a base-volume ratio against preceding candles, not USDT turnover, '
    'a percentage, buying pressure, or measured order flow. Prefer RSI {{rsi_display}} when needed; '
    'these display tokens are derived from the same stored observation. Use at most a couple '
    'of relevant measurements; omit an indicator that adds no explanation. Price changes must '
    'state their supplied window and percent unit: hourly change {{change_1h}}% or '
    'daily change {{change_24h}}%, only when the respective token exists. Daily is a rolling '
    'closed-candle window, not necessarily today\'s calendar session. Support/resistance are '
    'local recorded range levels unless explicitly marked prior-day. RSI and MACD are '
    'indicator readings, not bounds on price, guarantees, or confirmation of capitulation. '
    'Candles and volume alone cannot confirm capitulation, exhaustion, motives or causation. '
    'Explain competing interpretations conditionally. End the reasoning with a useful '
    'reassessment condition; do not echo the opening question or pad with generic warnings. '
    'Do not include platform endorsement, "Binance spot proof", internal approval statuses, '
    'dashboard instructions or audit markers in the authored prose. '
)


def display_facts(values):
    """Explicit display aliases; original prices/levels and evidence remain exact."""
    result = dict(values)
    for key, places in (('relative_volume', 2), ('rsi', 1)):
        if key in values:
            text = format(float(values[key]), f'.{places}f')
            # A small nonzero ratio must not be presented as zero.
            if float(text) == 0 and float(values[key]) != 0:
                text = values[key]
            result[key + '_display'] = text
    return result


def _field(key):
    return key.removesuffix('_display')


def _expected(label):
    label = label.lower()
    if label in ('relative volume', 'volume ratio'):
        return 'relative_volume'
    if label.startswith('daily '):
        return 'change_24h'
    if label.startswith('hourly '):
        return 'change_1h'
    return label


def findings(text, facts=None, *, tokens=False):
    """Catch explicit known failure patterns; no rewriting of unsupported facts."""
    errors = []
    for match in re.finditer(r'\bbounds? (?:for|of|on) (?:the |a |current )*(?:trend|price)\b', text, re.I):
        before = text[max(0, match.start() - 220):match.start()]
        same_clause = re.search(r'\b(?:RSI|MACD)\b[^.!?\n]*$', before, re.I)
        prior_reference = re.search(r'\b(?:RSI|MACD)\b[^.!?\n]{0,140}[.!?]\s*(?:These|Those|They)\s+are\s*$', before, re.I)
        negated = re.search(r'\b(?:not|never|no)\s+(?:the\s+)?$', before, re.I)
        if (same_clause or prior_reference) and not negated:
            errors.append('Indicators are readings, not price or trend bounds')
    if re.search(r'\b(?:confirms?|proves?|guarantees?)\s+(?:a |the )?capitulation\b|'
                 r'\bcapitulation\s+(?:is\s+)?(?:confirmed|proven|guaranteed)\b', text, re.I):
        # Do not reject an explicit limitation such as "cannot confirm capitulation".
        assertions = re.sub(r'\b(?:cannot|does not|do not|not enough to|never)\s+'
                            r'(?:confirm|prove|guarantee)\s+(?:a |the )?capitulation', '', text, flags=re.I)
        if re.search(r'\b(?:confirms?|proves?|guarantees?)\s+(?:a |the )?capitulation\b|'
                     r'\bcapitulation\s+(?:is\s+)?(?:confirmed|proven|guaranteed)\b', assertions, re.I):
            errors.append('Candles and volume do not confirm capitulation')
    if re.search(r'\bBinance\s+spot\s+proof\b|Validated draft approved;|'
                 r'awaiting scheduler and publication gates|Dashboard:\s*Preview chart', text, re.I):
        errors.append('Internal publication or proof text is not public prose')

    for match in LABEL.finditer(text):
        value = match['value']
        expected = _expected(match['label'])
        token = TOKEN.fullmatch(value)
        if token and _field(token[1]) != expected:
            errors.append('Metric label does not match its evidence token')
        elif not token and facts and expected in facts:
            candidates = {facts[expected], facts.get(expected + '_display')}
            if value not in candidates:
                errors.append('Metric label does not match its recorded value')
        if expected.startswith('change_') and not re.match(r'\s*(?:%|percent\b)', text[match.end():], re.I):
            errors.append('Price change requires a percent unit and its recorded window')

    if tokens:
        for match in TOKEN.finditer(text):
            key = _field(match[1])
            before = text[max(0, match.start() - 80):match.start()]
            after = text[match.end():match.end() + 70]
            if key == 'relative_volume':
                label = re.search(r'\b(?:relative volume|volume ratio|relvol)\b[^.!?\n]{0,50}$', before, re.I)
                unit = re.match(r'\s*(?:x|×|times)\s*(?:the\s+)?(?:baseline|preceding|average)', after, re.I)
                if not label and not unit:
                    errors.append('Relative volume must be labelled as a baseline ratio')
                if re.match(r'\s*(?:%|percent\b|USDT\b|USD\b)', after, re.I):
                    errors.append('Relative volume is a ratio, not percent or turnover')
                if re.search(r'\b(?:next|following|future)\s+(?:closed\s+)?candle\b[^.!?\n]*$', before, re.I) and not re.search(r'\b(?:current|recorded|observed)\b[^.!?\n]*$', before, re.I):
                    errors.append('A current measurement cannot be assigned to a future candle')
            if key.startswith('change_'):
                if not re.match(r'\s*(?:%|percent\b)', after, re.I):
                    errors.append('Price change requires a percent unit and its recorded window')
                windows = {'15m': r'quarter.hour|fifteen.minute', '1h': r'hourly|past hour|hour.over.hour',
                           '4h': r'four.hour', '24h': r'daily|past day|rolling day|day.over.day',
                           '3d': r'three.day', '7d': r'weekly|past week|seven.day'}
                window = windows.get(key[7:])
                if window and not re.search(r'\b(?:' + window + r')\b', before, re.I):
                    errors.append('Price change requires a percent unit and its recorded window')
    elif facts:
        ratios = {facts.get('relative_volume'), facts.get('relative_volume_display')} - {None}
        for match in re.finditer(r'\bvolume\s+(?:is|of|at)\s+(' + NUMBER + r')(?!\w|\.\d)', text, re.I):
            if not re.search(r'\brelative\s+$', text[max(0, match.start() - 15):match.start()], re.I) and match[1] in ratios:
                errors.append('Relative volume must be labelled as a baseline ratio')
    return list(dict.fromkeys(errors))
