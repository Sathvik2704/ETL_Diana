from __future__ import annotations

from dataclasses import dataclass

import llm

from .profiler import DatasetProfile


@dataclass(frozen=True)
class TransformationPlan:
    goal: str


class PlannerAgent:
    """
    Placeholder planner agent. In this prototype, the goal is already the plan.
    """

    def plan(self, goal: str) -> TransformationPlan:
        return TransformationPlan(goal=goal)


class CoderAgent:
    """
    Uses LLMs via `llm.py` to generate executable pandas code.
    Can accept an error message to iteratively debug and fix code.
    """

    def generate_code(self, plan: TransformationPlan, profile: DatasetProfile, error_msg: str | None = None) -> str:
        return llm.generate_transformation(
            plan.goal,
            profile.columns,
            value_hints=profile.value_hints,
            dtypes=profile.dtypes,
            error_msg=error_msg,
        )

