from dataclasses import dataclass
from typing import Optional


@dataclass
class User:
    id: str
    email: str
    password: Optional[str] = None
    refresh_token: Optional[str] = None
    name: Optional[str] = None


@dataclass
class RegisterForm:
    email: str
    password: str


@dataclass
class Token:
    refresh_token: str
