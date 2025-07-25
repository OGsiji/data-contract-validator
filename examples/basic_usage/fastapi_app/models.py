"""
Example FastAPI models using Pydantic.
"""

from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class User(BaseModel):
    """User model - should match DBT users table."""
    user_id: str
    email: str
    first_name: str
    last_name: str
    created_at: datetime
    is_active: bool

class UserProfile(BaseModel):
    """User profile - might have missing fields."""
    user_id: str
    email: str
    full_name: str  # This won't exist in DBT!
    bio: Optional[str] = None
