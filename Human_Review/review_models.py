from pydantic import BaseModel
from typing import Optional

class Reviewitem(BaseModel):
    """
    This class defines what a single review item looks like.
    It is used when SHOWING rows to a human reviewer.
    """

    employee_id: str
    issue_reason: str
    current_score: int
    suggested_action: str

class ReviewDecision(BaseModel):
    """
    This class defines what a single review item looks like.
    It is used when SHOWING rows to a human reviewer.
    """

    employee_id: str
    decision: str
    review_notes: Optional[str] = None