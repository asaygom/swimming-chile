import re
import unicodedata


def normalize_search_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.casefold())
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def search_tokens(value: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", normalize_search_text(value))
    return list(dict.fromkeys(tokens))


def normalized_search_sql(expression: str) -> str:
    return f"""
    TRANSLATE(
        LOWER({expression}),
        'áàäâãéèëêíìïîóòöôõúùüûñ',
        'aaaaaeeeeiiiiooooouuuun'
    )
    """


def build_token_search_clause(
    expressions: list[str], tokens: list[str]
) -> tuple[str, list[str]]:
    token_clauses = []
    params = []

    for token in tokens:
        expression_clauses = [
            f"{normalized_search_sql(expression)} LIKE %s"
            for expression in expressions
        ]
        token_clauses.append(f"({' OR '.join(expression_clauses)})")
        params.extend([f"%{token}%"] * len(expressions))

    return " AND ".join(token_clauses), params
