"""Narrow, explainable text gates; no rewriting or claim of complete moderation."""
import re

def community_findings(text):
    text=str(text)
    findings=[]
    # Direct instant-message invitations/links; educational references alone pass.
    if re.search(r'(?i)(?:https?://)?(?:t\.me/|wa\.me/|chat\.whatsapp\.com/|discord\.gg/)',text) or re.search(
        r'(?i)\b(?:join|dm|contact|message|follow)\s+(?:me|us|our|my)?\s*(?:on\s+)?(?:telegram|whatsapp|discord)\b',text):
        findings.append('Community review required: external contact promotion')
    for sentence in re.split(r'[.!?\n]+',text):
        if re.search(r'(?i)\b(?:never|do not|don.t|avoid|beware)\b',sentence):continue
        if re.search(r'(?i)\b(?:send|share|provide|enter)\s+(?:me\s+|us\s+)?(?:your\s+)?(?:seed phrase|private key|password|login credentials)\b',sentence):
            findings.append('Community review required: sensitive-information request')
        if re.search(r'(?i)\b(?:borrow|rent|buy|sell)\s+(?:a\s+|your\s+|someone.s\s+)?(?:verified\s+)?(?:binance\s+)?account\b',sentence):
            findings.append('Community review required: account misuse')
        if re.search(r'(?i)\b(?:guaranteed|officially guaranteed)\b.{0,60}\b(?:rebate|cashback|referral reward)\b',sentence):
            findings.append('Community review required: misleading reward promise')
    return list(dict.fromkeys(findings))
