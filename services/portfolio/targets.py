from __future__ import annotations

import ast
import re
from dataclasses import dataclass


TARGET_VARIABLES = frozenset({"BPS", "EPS", "DPS", "보유지분", "본주가격", "매입가"})
_NUMBER_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")


@dataclass(frozen=True)
class TargetInput:
    price: float | None
    formula: str | None


def normalize_target_formula(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def parse_target_input(value: object) -> TargetInput:
    raw = str(value or "").strip()
    if not raw:
        return TargetInput(price=None, formula=None)

    numeric_candidate = raw.replace(",", "")
    if _NUMBER_RE.fullmatch(numeric_candidate):
        price = float(numeric_candidate)
        if price < 0:
            raise ValueError("목표가는 0 이상이어야 합니다.")
        return TargetInput(price=price, formula=None)

    formula = normalize_target_formula(raw)
    validate_target_formula(formula)
    return TargetInput(price=None, formula=formula)


def validate_target_formula(formula: str) -> None:
    if not formula:
        return
    try:
        tree = ast.parse(formula, mode="eval")
    except SyntaxError as exc:
        raise ValueError("목표가 수식 형식이 올바르지 않습니다.") from exc
    _validate_node(tree)


def _validate_node(node: ast.AST) -> None:
    if isinstance(node, ast.Expression):
        _validate_node(node.body)
        return
    if isinstance(node, ast.BinOp):
        if not isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div)):
            raise ValueError("목표가 수식에는 +, -, *, / 만 사용할 수 있습니다.")
        _validate_node(node.left)
        _validate_node(node.right)
        return
    if isinstance(node, ast.UnaryOp):
        if not isinstance(node.op, (ast.UAdd, ast.USub)):
            raise ValueError("목표가 수식에는 단항 +, - 만 사용할 수 있습니다.")
        _validate_node(node.operand)
        return
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return
    if isinstance(node, ast.Name):
        if node.id not in TARGET_VARIABLES:
            allowed = ", ".join(sorted(TARGET_VARIABLES))
            raise ValueError(f"사용할 수 없는 변수입니다: {node.id}. 허용 변수: {allowed}")
        return
    raise ValueError("목표가 수식에는 숫자, 허용 변수, 사칙연산, 괄호만 사용할 수 있습니다.")
