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
    password: str
    email: Optional[str] = None
    login: Optional[str] = None


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
class Posts:
    previous_ids: Optional[List[str]]
    top: int

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
class UserPhoto:
    photo_ids: Optional[List[str]] = None
    photo_path: Optional[List[str]] = None

@dataclass
class UserInfo:
    username: str
    age: int
    describe: str

@dataclass
class TgUserReaction:
    tg_user_id: int
    reaction: str
    post_id: str
    book_name: str

@dataclass
class UserReaction:
    reaction: str
    post_id: str

@dataclass
class GenerationLogin:
    nickname: str

@dataclass
class Favorites:
    post_id: str