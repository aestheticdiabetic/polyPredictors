"""
Market categorization utilities.

Classifies a bet's market question into:
  - sport    : Soccer | Basketball | Tennis | eSports | American Football |
               Baseball | Politics | Crypto | Other
  - bet_type : Over/Under | Spread | Exact Score | Moneyline | Match Winner | Other
"""

import re
from typing import Tuple

# ---------------------------------------------------------------------------
# Bet-type detection
# ---------------------------------------------------------------------------

def classify_bet_type(question: str) -> str:
    """Return the structural bet type inferred from the market question."""
    q = question.strip()
    if re.search(r'\bO/U\b', q, re.IGNORECASE):
        return "Over/Under"
    if re.match(r'^Spread\s*:', q, re.IGNORECASE):
        return "Spread"
    if re.match(r'^Exact\s+Score\s*:', q, re.IGNORECASE):
        return "Exact Score"
    if re.search(r'\bWill .+\bwin\b', q, re.IGNORECASE):
        return "Moneyline"
    if re.search(r'\bWinner\b', q, re.IGNORECASE):
        return "Match Winner"
    if re.search(r' vs\.? ', q, re.IGNORECASE):
        return "Match Winner"
    return "Other"


# ---------------------------------------------------------------------------
# Sport / topic detection
# ---------------------------------------------------------------------------

_OU_RE = re.compile(r'\bO/U\s+([\d.]+)', re.IGNORECASE)
_SPREAD_RE = re.compile(r'Spread\s*:.*\(\s*[+-]?\s*([\d.]+)\s*\)', re.IGNORECASE)

_ESPORTS = {
    "COUNTER-STRIKE", "CS:", "CSGO", "CS2", "LEAGUE OF LEGENDS", "VALORANT",
    "DOTA", "OVERWATCH", "STARCRAFT", "ROCKET LEAGUE", "RAINBOW SIX",
    "MAP 1 WINNER", "MAP 2 WINNER", "MAP 3 WINNER", " MAP 1 ", " MAP 2 ", " MAP 3 ",
    "PARIVISION", "NATUS VINCERE", "NAVI ", "ASTRALIS", "FNATIC", "VITALITY",
    "FAZE CLAN", "TEAM LIQUID", "G2 ESPORTS", "NIP ", "NINJAS IN PYJAMAS",
}

_POLITICS = {
    "ELECTION", "PRESIDENT", "SENATOR", "SENATE", "CONGRESS", "DEMOCRAT",
    "REPUBLICAN", "TRUMP", "BIDEN", "HARRIS", "VOTE", "BALLOT", "GOVERNOR",
    "PRIME MINISTER", "REFERENDUM", "PARLIAMENT", "CABINET", "PRESIDENCY",
    "INAUGURATION", "IMPEACH",
}

_CRYPTO = {
    "BITCOIN", "ETHEREUM", "BTC/", "ETH/", "CRYPTO", "BLOCKCHAIN",
    "ALTCOIN", "DEFI", "SOLANA", "SOL/", "DOGECOIN", "XRP", "CARDANO",
    "BINANCE", "COINBASE", "UNISWAP",
}

_NBA_TEAMS = {
    "LAKERS", "CELTICS", "WARRIORS", "NETS", "KNICKS", "BULLS", "HEAT",
    "PISTONS", "BUCKS", "RAPTORS", "NUGGETS", "SUNS", "CLIPPERS", "SPURS",
    "ROCKETS", "MAVERICKS", "THUNDER", "BLAZERS", "TIMBERWOLVES", "JAZZ",
    "PELICANS", "HAWKS", "HORNETS", "MAGIC", "PACERS", "76ERS",
    "WIZARDS", "GRIZZLIES", "KINGS",
}

_COLLEGE_BB = {
    "BADGERS", "WOLVERINES", "SPARTANS", "BUCKEYES", "HAWKEYES",
    "TAR HEELS", "COMMODORES", "BOILERMAKERS", "HOYAS", "FRIARS",
    "MOUNTAINEERS", "VOLUNTEERS", "CORNHUSKERS", "TERRAPINS",
    "RAZORBACKS", "GATORS", "HURRICANES", "SOONERS", "JAYHAWKS",
}

_NFL_TEAMS = {
    "PATRIOTS", "COWBOYS", "STEELERS", "PACKERS", "49ERS", "CHIEFS",
    "RAVENS", "BRONCOS", "SEAHAWKS", "FALCONS", "SAINTS", "GIANTS",
    "JETS", "EAGLES", "CHARGERS", "RAMS", "BENGALS", "BROWNS", "COLTS",
    "TITANS", "JAGUARS", "TEXANS", "DOLPHINS", "RAIDERS", "LIONS",
    "BUCCANEERS", "VIKINGS", "COMMANDERS",
}

_NFL_KEYWORDS = {"NFL", "SUPER BOWL", "AFC CHAMPIONSHIP", "NFC CHAMPIONSHIP"}

_MLB_TEAMS = {
    "RED SOX", "YANKEES", "DODGERS", "CUBS", "ASTROS", "BRAVES", "METS",
    "WHITE SOX", "REDS", "PIRATES", "BREWERS", "ROCKIES", "ORIOLES",
    "BLUE JAYS", "NATIONALS", "MARLINS", "RAYS", "ANGELS", "RANGERS",
    "PADRES", "PHILLIES", "MARINERS", "TIGERS", "ROYALS", "TWINS",
    "ATHLETICS", "DIAMONDBACKS",
}

_MLB_KEYWORDS = {"MLB", "WORLD SERIES", " BASEBALL"}

_TENNIS_KEYWORDS = {
    "ATP ", "WTA ", "ROLAND GARROS", "WIMBLEDON", "US OPEN",
    "AUSTRALIAN OPEN", "MIAMI OPEN", "MADRID OPEN", "ROME OPEN",
    "INDIAN WELLS", "CINCINNATI", "CHALLENGER", "ITF ", "DAVIS CUP",
    "ASUNCION", "MONTERREY", "BOGOTA", "BUDAPEST", "MARRAKECH",
    "HOUSTON", "ESTORIL", "MUNICH OPEN", "BARCELONA OPEN",
}

_SOCCER_KEYWORDS = {
    "PREMIER LEAGUE", "LA LIGA", "SERIE A", "BUNDESLIGA", "EREDIVISIE",
    "LIGUE 1", "CHAMPIONS LEAGUE", "EUROPA LEAGUE", "CONFERENCE LEAGUE",
    "WORLD CUP", "COPA DEL REY", "FA CUP", "CARABAO CUP", "SCOTTISH PREMIERSHIP",
    "PRIMEIRA LIGA", "SUPER LIG",
}

_SOCCER_TEAM_NAMES = {
    "BALOMPI", "ARSENAL", "CHELSEA", "LIVERPOOL", "TOTTENHAM", "EVERTON",
    "MANCHESTER", "LEICESTER", "ASTON VILLA", "CRYSTAL PALACE", "NEWCASTLE",
    "WEST HAM", "WOLVERHAMPTON", "BRIGHTON", "BRENTFORD", "FULHAM",
    "BOLOGNA", "NAPOLI", "LAZIO", "FIORENTINA", "ATALANTA", "AS ROMA",
    "JUVENTUS", "AC MILAN", "INTER MILAN", "TORINO", "UDINESE",
    "BARCELONA", "SEVILLA", "VALENCIA", "VILLARREAL", "BETIS", "CELTA",
    "ATLETICO", "REAL BETIS", "REAL MADRID", "REAL SOCIEDAD",
    "PANATHINAIKOS", "OLYMPIAKOS", "AEK", "PAOK",
    "AJAX", "PSV", "FEYENOORD", "TWENTE",
    "PORTO", "BENFICA", "SPORTING CP", "BRAGA",
    "RANGERS", "CELTIC", "HEARTS", "HIBERNIAN",
    "NOTTINGHAM FOREST", "MIDTJYLLAND", "SHAKHTAR", "LECH POZNA",
    "1. FSV MAINZ", "SIGMA OLOMOUC", "AC SPARTA", "VFB STUTTGART",
    "DORTMUND", "LEVERKUSEN", "HOFFENHEIM",
    "KAYSERISPOR", "FATIH KARA",
}

_SOCCER_FC_SUFFIXES = (" FC", "FC ", " CF", " SC ", " RFC", " AFC")


def classify_sport(question: str) -> str:
    """Return the sport/topic inferred from the market question."""
    q = question.upper()

    # eSports — check before other sports; team names can be ambiguous
    if any(kw in q for kw in _ESPORTS):
        return "eSports"

    # Politics
    if any(kw in q for kw in _POLITICS):
        return "Politics"

    # Crypto
    if any(kw in q for kw in _CRYPTO):
        return "Crypto"

    # O/U total is a very strong numeric signal
    ou_match = _OU_RE.search(question)
    if ou_match:
        total = float(ou_match.group(1))
        if total >= 100:
            return "Basketball"
        if 20 <= total < 100:
            return "American Football"
        # total < 20 — could be soccer/hockey; fall through to name checks

    # Spread value signal for ambiguous spread questions
    spread_match = _SPREAD_RE.search(question)
    if spread_match:
        spread_val = float(spread_match.group(1))
        if spread_val >= 7:
            if any(t in q for t in _NBA_TEAMS | _COLLEGE_BB):
                return "Basketball"
            if any(t in q for t in _NFL_TEAMS) or any(kw in q for kw in _NFL_KEYWORDS):
                return "American Football"

    # Basketball — NBA + college team names
    if any(t in q for t in _NBA_TEAMS | _COLLEGE_BB):
        return "Basketball"

    # American Football
    if any(kw in q for kw in _NFL_KEYWORDS) or any(t in q for t in _NFL_TEAMS):
        return "American Football"

    # Baseball
    if any(kw in q for kw in _MLB_KEYWORDS) or any(t in q for t in _MLB_TEAMS):
        return "Baseball"

    # Tennis — tournament names or "Location: Player vs Player" pattern
    if any(kw in q for kw in _TENNIS_KEYWORDS):
        return "Tennis"
    if re.match(r'^[A-Za-z\s\-]+:\s+[A-Z][a-z]+ [A-Z][a-z]+ vs', question):
        return "Tennis"

    # Soccer — competition keywords
    if any(kw in q for kw in _SOCCER_KEYWORDS):
        return "Soccer"
    # Soccer — known team name fragments
    if any(name in q for name in _SOCCER_TEAM_NAMES):
        return "Soccer"
    # Soccer — team suffix heuristic
    if any(q.find(sfx) != -1 for sfx in _SOCCER_FC_SUFFIXES):
        return "Soccer"
    # Low O/U (< 10) with no other match → soccer
    if ou_match and float(ou_match.group(1)) < 10:
        return "Soccer"

    return "Other"


def classify(question: str) -> Tuple[str, str]:
    """Return (sport, bet_type) for a given market question."""
    return classify_sport(question), classify_bet_type(question)
