"""
Market categorization utilities.

Classifies a bet's market question into:
  - sport    : Soccer | Basketball | Tennis | eSports | American Football |
               Baseball | Hockey | Politics | Crypto | Other
  - bet_type : Over/Under | Spread | Exact Score | Moneyline | Match Winner | Other
"""

import re
from functools import lru_cache

# ---------------------------------------------------------------------------
# Bet-type detection
# ---------------------------------------------------------------------------


def classify_bet_type(question: str) -> str:
    """Return the structural bet type inferred from the market question."""
    q = question.strip()
    if re.search(r"\bO/U\b", q, re.IGNORECASE):
        return "Over/Under"
    if re.match(r"^Spread\s*:", q, re.IGNORECASE):
        return "Spread"
    if re.match(r"^Exact\s+Score\s*:", q, re.IGNORECASE):
        return "Exact Score"
    if re.search(r"\bWill .+\bwin\b", q, re.IGNORECASE):
        return "Moneyline"
    if re.search(r"\bWinner\b", q, re.IGNORECASE):
        return "Match Winner"
    if re.search(r" vs\.? ", q, re.IGNORECASE):
        return "Match Winner"
    return "Other"


# ---------------------------------------------------------------------------
# Sport / topic detection
# ---------------------------------------------------------------------------

_OU_RE = re.compile(r"\bO/U\s+([\d.]+)", re.IGNORECASE)
_SPREAD_RE = re.compile(r"Spread\s*:.*\(\s*[+-]?\s*([\d.]+)\s*\)", re.IGNORECASE)

_ESPORTS = {
    "COUNTER-STRIKE",
    "CS:",
    "CSGO",
    "CS2",
    "LEAGUE OF LEGENDS",
    "VALORANT",
    "DOTA",
    "OVERWATCH",
    "STARCRAFT",
    "ROCKET LEAGUE",
    "RAINBOW SIX",
    "MAP 1 WINNER",
    "MAP 2 WINNER",
    "MAP 3 WINNER",
    " MAP 1 ",
    " MAP 2 ",
    " MAP 3 ",
    "PARIVISION",
    "NATUS VINCERE",
    "NAVI ",
    "ASTRALIS",
    "FNATIC",
    "VITALITY",
    "FAZE CLAN",
    "TEAM LIQUID",
    "G2 ESPORTS",
    "NIP ",
    "NINJAS IN PYJAMAS",
}

_POLITICS = {
    "ELECTION",
    "PRESIDENT",
    "SENATOR",
    "SENATE",
    "CONGRESS",
    "DEMOCRAT",
    "REPUBLICAN",
    "TRUMP",
    "BIDEN",
    "HARRIS",
    "VOTE",
    "BALLOT",
    "GOVERNOR",
    "PRIME MINISTER",
    "REFERENDUM",
    "PARLIAMENT",
    "CABINET",
    "PRESIDENCY",
    "INAUGURATION",
    "IMPEACH",
}

_CRYPTO = {
    "BITCOIN",
    "ETHEREUM",
    "BTC/",
    "ETH/",
    "CRYPTO",
    "BLOCKCHAIN",
    "ALTCOIN",
    "DEFI",
    "SOLANA",
    "SOL/",
    "DOGECOIN",
    "XRP",
    "CARDANO",
    "BINANCE",
    "COINBASE",
    "UNISWAP",
}

_NBA_TEAMS = {
    "LAKERS",
    "CELTICS",
    "WARRIORS",
    "NETS",
    "KNICKS",
    "BULLS",
    "HEAT",
    "PISTONS",
    "BUCKS",
    "RAPTORS",
    "NUGGETS",
    "SUNS",
    "CLIPPERS",
    "SPURS",
    "ROCKETS",
    "MAVERICKS",
    "THUNDER",
    "BLAZERS",
    "TIMBERWOLVES",
    "JAZZ",
    "PELICANS",
    "HAWKS",
    "HORNETS",
    "MAGIC",
    "PACERS",
    "76ERS",
    "WIZARDS",
    "GRIZZLIES",
    "KINGS",
    "CAVALIERS",
}

_COLLEGE_BB = {
    "BADGERS",
    "WOLVERINES",
    "SPARTANS",
    "BUCKEYES",
    "HAWKEYES",
    "TAR HEELS",
    "COMMODORES",
    "BOILERMAKERS",
    "HOYAS",
    "FRIARS",
    "MOUNTAINEERS",
    "VOLUNTEERS",
    "CORNHUSKERS",
    "TERRAPINS",
    "RAZORBACKS",
    "GATORS",
    "HURRICANES",
    "SOONERS",
    "JAYHAWKS",
    # Additional college basketball team nicknames commonly seen on Polymarket
    "CRIMSON TIDE",  # Alabama
    "CYCLONES",  # Iowa State
    "WILDCATS",  # Kentucky, Arizona, K-State, many others
    "LONGHORNS",  # Texas
    "BLUE DEVILS",  # Duke
    "HUSKIES",  # UConn, Washington
    "WOLFPACK",  # NC State
    "BEARCATS",  # Cincinnati
    "AGGIES",  # Utah State, Texas A&M
    "NITTANY LIONS",  # Penn State
    "SEMINOLES",  # Florida State
    "CAVALIERS",  # Virginia (UVA)
}

_NFL_TEAMS = {
    "PATRIOTS",
    "COWBOYS",
    "STEELERS",
    "PACKERS",
    "49ERS",
    "CHIEFS",
    "RAVENS",
    "BRONCOS",
    "SEAHAWKS",
    "FALCONS",
    "SAINTS",
    "GIANTS",
    "JETS",
    "EAGLES",
    "CHARGERS",
    "RAMS",
    "BENGALS",
    "BROWNS",
    "COLTS",
    "TITANS",
    "JAGUARS",
    "TEXANS",
    "DOLPHINS",
    "RAIDERS",
    "LIONS",
    "BUCCANEERS",
    "VIKINGS",
    "COMMANDERS",
}

_NFL_KEYWORDS = {"NFL", "SUPER BOWL", "AFC CHAMPIONSHIP", "NFC CHAMPIONSHIP"}

_MLB_TEAMS = {
    "RED SOX",
    "YANKEES",
    "DODGERS",
    "CUBS",
    "ASTROS",
    "BRAVES",
    "METS",
    "WHITE SOX",
    "REDS",
    "PIRATES",
    "BREWERS",
    "ROCKIES",
    "ORIOLES",
    "BLUE JAYS",
    "NATIONALS",
    "MARLINS",
    "RAYS",
    "ANGELS",
    "PADRES",
    "PHILLIES",
    "MARINERS",
    "TIGERS",
    "ROYALS",
    "TWINS",
    "ATHLETICS",
    "DIAMONDBACKS",
    # Note: "RANGERS" omitted — ambiguous with NY Rangers (NHL); resolved by
    # checking NHL first. Texas Rangers context caught via _MLB_KEYWORDS.
}

_MLB_KEYWORDS = {"MLB", "WORLD SERIES", " BASEBALL", "TEXAS RANGERS"}

_NHL_TEAMS = {
    "BLUE JACKETS",
    "MAPLE LEAFS",
    "CANADIENS",
    "SENATORS",
    "JETS",
    "FLAMES",
    "OILERS",
    "CANUCKS",
    "GOLDEN KNIGHTS",
    "KRAKEN",
    "BRUINS",
    "SABRES",
    "RED WINGS",
    "PANTHERS",
    "LIGHTNING",
    "HURRICANES",
    "CAPITALS",
    "PENGUINS",
    "FLYERS",
    "DEVILS",
    "ISLANDERS",
    "NY RANGERS",
    "BLACKHAWKS",
    "PREDATORS",
    "BLUES",
    "WILD",
    "AVALANCHE",
    "STARS",
    "COYOTES",
    "DUCKS",
    "SHARKS",
}

_NHL_KEYWORDS = {"NHL", " HOCKEY", "STANLEY CUP", "POWER PLAY"}

_TENNIS_KEYWORDS = {
    "ATP ",
    "WTA ",
    "ROLAND GARROS",
    "WIMBLEDON",
    "US OPEN",
    "AUSTRALIAN OPEN",
    "MIAMI OPEN",
    "MADRID OPEN",
    "ROME OPEN",
    "INDIAN WELLS",
    "CINCINNATI",
    "CHALLENGER",
    "ITF ",
    "DAVIS CUP",
    "ASUNCION",
    "MONTERREY",
    "BOGOTA",
    "BUDAPEST",
    "MARRAKECH",
    "HOUSTON",
    "ESTORIL",
    "MUNICH OPEN",
    "BARCELONA OPEN",
    "SET HANDICAP",  # tennis-specific handicap bet format
}

_SOCCER_KEYWORDS = {
    "PREMIER LEAGUE",
    "LA LIGA",
    "SERIE A",
    "BUNDESLIGA",
    "EREDIVISIE",
    "LIGUE 1",
    "CHAMPIONS LEAGUE",
    "EUROPA LEAGUE",
    "CONFERENCE LEAGUE",
    "WORLD CUP",
    "COPA DEL REY",
    "FA CUP",
    "CARABAO CUP",
    "SCOTTISH PREMIERSHIP",
    "PRIMEIRA LIGA",
    "SUPER LIG",
}

_SOCCER_TEAM_NAMES = {
    # MLS teams whose names contain substrings that match other sports' team lists.
    # "RED BULLS" must be listed before "BULLS" is checked against _NBA_TEAMS.
    "RED BULLS",
    "BALOMPI",
    "ARSENAL",
    "CHELSEA",
    "LIVERPOOL",
    "TOTTENHAM",
    "EVERTON",
    "MANCHESTER",
    "LEICESTER",
    "ASTON VILLA",
    "CRYSTAL PALACE",
    "NEWCASTLE",
    "WEST HAM",
    "WOLVERHAMPTON",
    "BRIGHTON",
    "BRENTFORD",
    "FULHAM",
    "BOLOGNA",
    "NAPOLI",
    "LAZIO",
    "FIORENTINA",
    "ATALANTA",
    "AS ROMA",
    "JUVENTUS",
    "AC MILAN",
    "INTER MILAN",
    "TORINO",
    "UDINESE",
    "BARCELONA",
    "SEVILLA",
    "VALENCIA",
    "VILLARREAL",
    "BETIS",
    "CELTA",
    "ATLETICO",
    "REAL BETIS",
    "REAL MADRID",
    "REAL SOCIEDAD",
    "PANATHINAIKOS",
    "OLYMPIAKOS",
    "AEK",
    "PAOK",
    "AJAX",
    "PSV",
    "FEYENOORD",
    "TWENTE",
    "PORTO",
    "BENFICA",
    "SPORTING CP",
    "BRAGA",
    # "RANGERS" intentionally omitted: ambiguous with NY Rangers (NHL).
    # "Rangers FC" style is caught earlier by the _SOCCER_FC_SUFFIXES check.
    "CELTIC",
    "HEARTS",
    "HIBERNIAN",
    "NOTTINGHAM FOREST",
    "MIDTJYLLAND",
    "SHAKHTAR",
    "LECH POZNA",
    "1. FSV MAINZ",
    "SIGMA OLOMOUC",
    "AC SPARTA",
    "VFB STUTTGART",
    "DORTMUND",
    "LEVERKUSEN",
    "HOFFENHEIM",
    "KAYSERISPOR",
    "FATIH KARA",
    # Additional clubs commonly seen on Polymarket
    "HANNOVER",  # Hannover 96 (German 2. Bundesliga)
    "HUESCA",  # SD Huesca (Spanish football)
    "RACING CLUB",  # RC Lens, RC Strasbourg, Racing Club de Lens (French football)
    "ANGERS SCO",  # Angers SCO (French football) — "ANGERS" alone is a substring of "RANGERS"
    "PUMAS",  # Pumas de la UNAM (Liga MX)
    "TIGRES",  # Tigres de la UANL (Liga MX)
    "CRUZEIRO",  # Cruzeiro EC (Brazilian football)
    "SPORTING KANSAS",  # Sporting Kansas City (MLS)
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

    # Soccer team names that could collide with NBA/NHL name checks must be
    # resolved first (e.g. "RED BULLS" contains "BULLS" → would fire NBA check).
    if any(name in q for name in _SOCCER_TEAM_NAMES):
        return "Soccer"
    # Soccer FC-suffix heuristic early: " FC", "FC ", etc. almost always means football.
    if any(q.find(sfx) != -1 for sfx in _SOCCER_FC_SUFFIXES):
        return "Soccer"

    # Basketball — NBA + college team names
    if any(t in q for t in _NBA_TEAMS | _COLLEGE_BB):
        return "Basketball"

    # American Football
    if any(kw in q for kw in _NFL_KEYWORDS) or any(t in q for t in _NFL_TEAMS):
        return "American Football"

    # Hockey (NHL) — checked before Baseball to resolve RANGERS ambiguity
    # ("Rangers vs. Blue Jackets" → Blue Jackets is NHL-exclusive)
    if any(kw in q for kw in _NHL_KEYWORDS) or any(t in q for t in _NHL_TEAMS):
        return "Hockey"
    # Rangers without an NHL co-team → assume NHL (NY Rangers more common on Polymarket).
    # Guard: "Rangers FC" style names are Scottish football; the FC-suffix check above
    # already returned "Soccer" for those, so reaching here means no FC suffix present.
    if "RANGERS" in q and not any(kw in q for kw in _MLB_KEYWORDS):
        return "Hockey"

    # Baseball
    if any(kw in q for kw in _MLB_KEYWORDS) or any(t in q for t in _MLB_TEAMS):
        return "Baseball"

    # Tennis — tournament names or "Location: Player vs Player" pattern
    if any(kw in q for kw in _TENNIS_KEYWORDS):
        return "Tennis"
    if re.match(r"^[A-Za-z\s\-]+:\s+[A-Z][a-z]+ [A-Z][a-z]+ vs", question):
        return "Tennis"

    # Soccer — competition keywords
    if any(kw in q for kw in _SOCCER_KEYWORDS):
        return "Soccer"
    # Low O/U (< 10) with no other match → soccer
    if ou_match and float(ou_match.group(1)) < 10:
        return "Soccer"

    return "Other"


@lru_cache(maxsize=2048)
def classify(question: str) -> tuple[str, str]:
    """Return (sport, bet_type) for a given market question.

    Results are cached — the same question string always maps to the same
    category pair, so repeated calls (e.g. filter check + persistence in the
    same bet placement path) are free after the first call.
    """
    return classify_sport(question), classify_bet_type(question)
