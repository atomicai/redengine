from dataclasses import dataclass
from typing import Dict, Any, List, Optional

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
class userIdChat:
    user_id: str 

@dataclass
class Token:
    refresh_token: str

@dataclass
class RegisterFormTelegram:
    tg_user_id: int

@dataclass
class TgUserPhoto:
    tg_user_id: int
    photo_ids: Optional[List[str]] = None
    photo_path: Optional[List[str]] = None

@dataclass
class TgUserInfo:
    tg_user_id: int
    username: str
    age: int
    describe: str

@dataclass
class TgUserReaction:
    tg_user_id: int
    reaction: str
    post_id: str
    book_name: str