"""Reviewed evergreen analysis: no invented current events or numerical claims."""
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


def educational_payload(index):
    title, body = LESSONS[index % len(LESSONS)]
    return {'title': title, 'body': body, 'category': 'education', 'priority': 20,
            'risk': 'low', 'article': False, 'image': None, 'approved_at': None,
            'generated_by': 'reviewed_education', 'human_review_required': False}
