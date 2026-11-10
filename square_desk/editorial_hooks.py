"""Concise presentation, never a substitute for evidence or duplicate checks."""
import re

# These are editorial questions, not claims that a pattern is present now.
# Numerical teaching examples in the source bank are not live asset facts.
TITLES = {
    59: 'High Daily Volume Does Not Promise an Easy Exit',
    64: 'What a Crossover Can Tell You About the Past',
    68: 'A Win Rate Leaves Out the Size of the Losses',
    72: 'A Green Daily Change Can Hide a Rough Journey',
    94: 'The Same Relative Volume Can Mean Different Things',
    96: 'What Does a Safe Haven Label Actually Establish?',
    105: 'The Winners You See and the Accounts You Do Not',
    106: 'Does Every Price Move Need a Grand Story?',
    109: 'A Run of Red Candles Does Not Promise a Bounce',
    120: 'The Reference Point Can Change the Story',
    127: 'A Flashy Candle Can Steal Attention From the Trend',
    130: 'Trending Is a Label, Not Confirmation',
    143: 'Why the Same Move Can Feel Large or Small',
    145: 'Repetition Does Not Turn a Rumor Into Evidence',
    182: 'What Changes When You Zoom Out?',
    196: 'What an Experienced Analyst Might Still Miss',
    228: 'Judge an Exit by the Rule, Not Just the Next Candle',
    240: 'A Shorter Timeframe Needs Fresher Evidence',
    260: 'The Beginner Who Skips the Warm-Up',
    279: 'Structure and Volume Are Different Keys',
    288: 'A Tide Line Is a Record, Not a Forecast',
    302: 'What Would Price and Momentum Disagreement Mean?',
    303: 'When Short and Daily Windows Tell Different Stories',
    308: 'Turnover and Relative Volume Answer Different Questions',
    309: 'Can a Crossover Disagree With the Broader Trend?',
    313: 'Moving Average Gaps: Trend or Overextension?',
    314: 'A Short Pause Inside a Longer Move',
    315: 'What Does Neutral Momentum Leave Unanswered?',
    317: 'What Would a Sharp Move Near Support Establish?',
    320: 'The Intraday Move Needs a Longer View',
    321: 'What If Price Sits Between Its Moving Averages?',
    326: 'A Turn Before Resistance Is Still a Question',
    327: 'A Higher Low Does Not Reveal Who Bought',
    328: 'Different Windows Can Produce Different Rankings',
    329: 'Would Indicator Agreement Be Independent Evidence?',
    336: 'ATR Adds Scale, Not a Statistical Proof',
    342: 'Volume and RSI Do Not Measure the Same Thing',
    343: 'A Scenario Needs an Expiry Rule',
    349: 'What Would Make a Recovery More Convincing?',
    357: 'A Watchlist Leader Is Not the Whole Market Leader',
    364: 'Compare Volatility Before Comparing Percentage Moves',
    377: 'Direction and VWAP Position Can Tell Different Stories',
    381: 'A Benchmark Helps Only When Its Evidence Is Fresh',
    390: 'Are These Peers Being Compared on the Same Basis?',
    403: 'When a Short Trade Becomes a Long Thesis',
    404: 'Myth: Low RSI Owes You a Bounce',
    408: 'Just Watching Can Still Become an Impulsive Click',
    411: 'A Lower Price Does Not Automatically Mean Lower Risk',
    428: 'Zoom In Enough and a Wiggle Looks Like a Canyon',
    436: 'The Hypothetical Trend That Waits for Your Laptop to Close',
    459: 'A Short Chart Is Not an Easy Income Plan',
    474: 'Candles Cannot Identify Whales or Insiders',
}

QUESTIONS = {
    1: ('Keep the rule or revise it after a missed move?', 'Process or outcome: which are you judging?'),
    2: ('The headline or the underlying mechanism?', 'One clue or independent confirmation?'),
    3: ('New evidence or a familiar bias?', 'The reference point or the full picture?'),
    4: ('Recheck the evidence or keep the first interpretation?', 'A useful pause or a rushed conclusion?'),
    5: ('Clear invalidation or a stronger-looking entry?', 'Follow the plan or reassess its assumptions?'),
    6: ('Where does this analogy help, and where does it break?', 'The label or the mechanism behind it?'),
    7: ('A clean close or a retest: which would add evidence?', 'What would weaken this reading?'),
    8: ('Shared direction or different structure?', 'Same move, but the same risk?'),
    9: ('A useful rule or a familiar shortcut?', 'Have you changed the rule after seeing the candle?'),
    10: ('More confidence or clearer limitations?', 'A strong claim or evidence you can check?'),
}


def concept_title(concept):
    title = TITLES.get(concept['id'], concept['title'].replace('$SYMBOL:', '').replace('$SYMBOL', '').strip())
    # Existing word, claim, numeric and semantic checks still inspect the result.
    # We/our/never/always are not, by themselves, evidence of a false claim.
    return title.replace('\u2014', ': ').replace('\ufffd', ' ')[:110]


def preview_instruction():
    return (' Start the body with one or two concise complete sentences, aiming for a short first paragraph '
            'of about 140 rendered characters. Put {{ticker}} in its first sentence. '
            'Lead with the topic-specific question or a genuine tension supported by the supplied observations. '
            'If there is no contradiction, use a clear question rather than inventing a paradox. '
            'Do not claim what everyone, whales or the crowd is doing; do not promise a hidden secret, '
            'a breakout, a trap, a benefit or a larger audience. This is a readability target, not a reason '
            'to omit caveats, cut sentences, or change the required overall word count. ')


def readable_opening(body, symbol):
    # An evidence-bound cashtag, without another AI call or an invented hook.
    first = re.split(r'(?<=[.!?])\s+|\n', body, maxsplit=1)[0]
    return body if '$' + symbol in first else '$' + symbol + ': ' + body
