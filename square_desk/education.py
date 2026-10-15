"""Reviewed evergreen analysis: no invented current events or numerical claims."""
import re
LESSONS = [
    ('A breakout is a process, not just a line',
     "A price crossing resistance attracts attention because it changes the chart's immediate structure. The harder question is whether the market accepts that new area. A brief excursion can end with price back inside the range, leaving late buyers exposed.\n\n"
     'Look at the close, the subsequent response and the quality of participation together. Expanding volume can support an interpretation of stronger interest, but it cannot identify every participant or establish intent. A retest that holds gives different evidence from an immediate rejection.\n\n'
     'Write down what would weaken your interpretation before considering an entry. That keeps an observation from turning into an emotional commitment. The same discipline applies to breakdowns: crossing support and staying below it are different events.\n\n'
     'This is an educational framework, not a current market alert or a promise of returns. Missing evidence should reduce confidence rather than invite a stronger story.'),
    ('Why a large candle can still be difficult to trade',
     'A dramatic move makes the opportunity easy to see and the risk easy to underestimate. When the candle expands, the distance to a meaningful invalidation point may expand too. A nearby stop can become vulnerable to ordinary movement, while a distant stop can require a smaller position.\n\n'
     'The right comparison is between the plausible reward and the loss you would accept if the interpretation fails. Entry quality matters as much as direction. Chasing after a sharp move can leave poor geometry even when the broader thesis is reasonable.\n\n'
     'Liquidity, spread, execution costs and the speed of price changes also affect what a chart observation means in practice. A theoretical level is not a guaranteed fill.\n\n'
     'An interesting chart can belong on a watchlist without becoming a trade. Waiting for clearer structure is an analytical choice. This educational discussion does not describe a live setup and offers no assured outcome.'),
    ('Volume confirms participation, not a motive',
     'A burst of volume tells us that more activity occurred within the observed interval. It does not automatically tell us whether the move came from informed buying, short covering, liquidations or temporary speculation. Those explanations require additional evidence.\n\n'
     'Begin by comparing like with like: the same venue, instrument and interval. A base-asset volume figure differs from turnover expressed in the quote currency. Mixing those units can create a compelling but incorrect comparison.\n\n'
     'Next consider where the activity appeared. Trading near resistance has a different structural context from trading after a failed breakout. Price response after the burst may reveal whether participation persisted or faded.\n\n'
     'A useful research note separates the observable change from the possible interpretations. Derivatives and order-book data may strengthen context when verified, but missing feeds do not justify inventing them. This is general education, not a live volume alert or an investment recommendation.'),
]

LESSONS += [
    ('A missed move does not create a debt',
     'Watching a coin rise without you can feel like losing something you already owned. That feeling is understandable, but the missed profit was never part of your account. Treating it as a debt can make the next decision about emotional repair rather than current evidence.\n\n'
     'A useful question is whether the same chart would interest you if you had not seen its earlier move. If the answer changes, regret may be influencing the interpretation. The chart can remain interesting while the entry becomes less attractive.\n\n'
     'Write down the observation, the assumption and the condition that would weaken it. This gives the next decision a present-tense basis. A watchlist is allowed to contain opportunities you never act on.\n\n'
     'The lesson is not to ignore momentum. It is to avoid making a future choice pay for an imagined past loss. This is an educational thought experiment, not a live setup or a promised result.'),
    ('A plan can lose its shape without the chart changing',
     'Imagine researching a trade calmly, then seeing an exciting post about the same asset. The chart has barely changed, but the entry suddenly feels urgent and the original waiting rule seems less important. The new information might be useful, yet its emotional effect deserves a separate check.\n\n'
     'This is where a written rule helps. Compare what the post actually adds with what it merely makes you feel. A confident voice is not fresh market evidence. A familiar ticker is not confirmation of your particular thesis.\n\n'
     'Ask what would justify changing the plan if the author were anonymous. If the answer depends mainly on popularity, pause and return to the observation itself.\n\n'
     'Flexibility means responding to relevant evidence. It does not require rewriting a rule whenever the feed becomes persuasive. The distinction is a research habit, not a guarantee that any trade will succeed.'),
    ('The checklist stayed home while the candle went out',
     'A hypothetical trader spends an evening building a careful checklist. Then a fast candle appears and the checklist is suddenly treated like optional reading. The funny part is familiar: preparation can feel important right up to the moment excitement asks for a shortcut.\n\n'
     'The lesson sits underneath the joke. A checklist is useful when it changes a decision, not simply when it exists. Keep the most important questions close enough to use: what is observed, what is assumed and what would invalidate the idea?\n\n'
     'An impressive candle does not answer every question about execution, liquidity or risk. It can make those questions more urgent precisely because the screen looks exciting.\n\n'
     'Humour can make an impulsive habit easier to notice without shaming anyone for having it. This is a fictional learning example, not a report of personal trades or a suggestion to chase a current move.'),
    ('A smooth price chart can hide a messy exit',
     'A chart compresses completed trades into a clean visual history. Your own order must still meet available buyers or sellers when you act. This difference helps explain why an attractive line on a screen does not guarantee an attractive fill.\n\n'
     'Liquidity is about the ability to trade with limited price impact. Spread is the gap between available buying and selling quotes. Both can change quickly, especially when attention concentrates on a thin market. Historical volume alone cannot tell you the exact cost of the next order.\n\n'
     'Think of a busy road with a narrow exit. Activity can be high while leaving at a particular moment remains difficult. The analogy is imperfect, but it separates participation from immediate execution.\n\n'
     'Before treating a chart level as an achievable trade, ask what evidence you have about the market around it. This explanation teaches a mechanism; it does not claim that any specific asset currently has an execution problem.'),
    ('Does a popular opinion count as confirmation?',
     'A repeated opinion can begin to sound like independent confirmation even when every version comes from the same original claim. In crypto feeds, familiar wording and rising attention can make this effect hard to notice. Repetition tells you something about distribution; it does not automatically add evidence.\n\n'
     'Start with a simple distinction. Did the new post provide a new observation, a different source or merely a stronger tone? Agreement may be useful context, but it cannot replace checking the premise.\n\n'
     'A fair test is to describe what would weaken the popular view. If that possibility is never discussed, the conversation may be rewarding certainty more than understanding.\n\n'
     'You do not have to become automatically contrarian. A widely shared idea can be correct. The goal is to understand why it deserves confidence rather than borrowing confidence from the size of the crowd. This is general media literacy, not a verdict on a current market narrative.'),
    ('What does a crypto creator owe a reader?',
     'A helpful crypto post gives the reader room to think. It separates the observation from the interpretation and makes uncertainty visible. A persuasive caption may attract attention, but trust depends on whether the explanation remains fair when the market moves differently.\n\n'
     'Creator incentives can complicate this relationship. When attention or activity may be rewarded, a writer should be especially careful about urgency, selective outcomes and unsupported certainty. The existence of an incentive does not prove bad intent; it does make transparency useful.\n\n'
     'Consider whether the post explains a failure condition as clearly as its preferred scenario. Does it help someone understand a decision, or mainly encourage a quick emotional reaction?\n\n'
     'A creator does not need to predict every move to be useful. Clear reasoning and honest limits can remain valuable after an exciting headline fades. This is an ethical discussion, not a claim about current platform payouts or a promise of income.'),
    ('Price and project value answer different questions',
     'A low token price can look cheap because the number is small. A high price can look expensive for the opposite reason. Neither impression tells you enough about the underlying project, circulating supply, liquidity or the rights attached to holding the token.\n\n'
     'Price answers what the latest market exchange valued a unit at. Project research asks a broader set of questions about use, incentives, design and uncertainty. These questions interact, but they are not interchangeable.\n\n'
     'A useful analogy is comparing the price of a slice without knowing the size of the whole cake. It helps explain the limitation of the unit price, while reminding us that an analogy is not a valuation model.\n\n'
     'Separate the familiar screen number from the research you still need. A price move can be worth watching without proving that a project became more valuable. This explanation makes no claim about the fundamentals, supply or future return of a particular coin.'),
    ('Changing your mind can be a sign of good research',
     'It is easy to treat a public opinion as something that must be defended. After explaining a market idea, changing the interpretation can feel like losing an argument. Research works better when the idea is allowed to respond to relevant evidence.\n\n'
     'Define the condition that would make your view weaker before the outcome is known. This turns reassessment into part of the original method rather than a convenient story added afterward. It also helps readers distinguish an updated premise from an unexplained reversal.\n\n'
     'Imagine the preferred scenario fails while an alternative remains plausible. The useful question is what changed, not who deserves blame. Describe the observation and the revised uncertainty without pretending the earlier outcome was inevitable.\n\n'
     'Consistency in method matters more than permanent loyalty to a conclusion. This is a lesson about reasoning and accountability, not evidence that any current asset has changed direction or that flexibility guarantees a better financial outcome.'),
]

DEPTH = [
    'A practical exercise is to write the observation separately from its interpretation. Then write an alternative explanation that fits the same observation. Finally, describe what additional evidence would help distinguish them. This can reveal when an attractive story is carrying more weight than the available data. The exercise does not produce certainty; it makes the assumptions easier to inspect. Keep the example hypothetical unless fresh evidence supports a current market claim.',
    'Try describing the decision to someone who has not seen the exciting headline. Explain what is known, what remains unclear and why the unanswered part matters. If the explanation depends on a confident personality or an impressive past result, check whether that detail actually supports the present question. A clear explanation should survive the removal of persuasive decoration. This is a thinking exercise, not a recommendation to act on an asset.',
    'A second exercise is to imagine the interpretation failing without assuming anyone behaved irrationally. What reasonable alternative might explain the outcome? This perspective can reduce hindsight bias and make it easier to discuss uncertainty honestly. It also keeps a conditional scenario from becoming a personal promise. The purpose is to improve the explanation of a decision, while recognising that improved reasoning cannot remove market risk or guarantee an outcome.',
]


def educational_payload(index, settings=None):
    title, body = LESSONS[index % len(LESSONS)]
    if settings:
        question = ('What would help you separate the observation from the story?',
                    'Which assumption would you check before acting on this idea?',
                    'What would make you revise your interpretation?')[index % 3]
        suffix = question + '\n\n#CryptoLearning #' + ('TradingPsychology', 'RiskManagement', 'CryptoEducation')[index % 3]
        for offset in range(len(DEPTH)):
            if len((body + ' ' + suffix).split()) >= settings.post_min_words:
                break
            addition = DEPTH[(index + offset) % len(DEPTH)]
            paragraph = []
            for sentence in re.split(r'(?<=[.!?])\s+', addition):
                candidate = body + '\n\n' + ' '.join(paragraph + [sentence]) + '\n\n' + suffix
                if (len(candidate.split()) <= settings.post_max_words
                        and len((title + '\n\n' + candidate).encode('utf-16-le')) // 2 <= settings.post_max_characters):
                    paragraph.append(sentence)
                if len((body + ' ' + ' '.join(paragraph) + ' ' + suffix).split()) >= settings.post_min_words:
                    break
            if paragraph:
                body += '\n\n' + ' '.join(paragraph)
        body += '\n\n' + suffix
    return {'title': title, 'body': body, 'category': 'education', 'priority': 20,
            'risk': 'low', 'article': False, 'image': None, 'approved_at': None,
            'generated_by': 'reviewed_education', 'human_review_required': False}
