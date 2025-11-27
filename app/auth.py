from __future__ import annotations

import secrets
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from passlib.context import CryptContext
from sqlmodel import Session, select

from app.database import get_session
from app.models import Upload, UploadStatus, User, UserRole

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def authenticate_user(session: Session, email: str, password: str) -> Optional[User]:
    normalized_email = email.strip().lower()
    statement = select(User).where(User.email == normalized_email, User.is_active == True)  # noqa: E712
    user = session.exec(statement).first()
    if not user:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


def get_user_by_role(session: Session, role: UserRole) -> Optional[User]:
    statement = (
        select(User)
        .where(User.role == role, User.is_active == True)  # noqa: E712
        .order_by(User.created_at.asc())
    )
    return session.exec(statement).first()


def create_user(
    session: Session,
    *,
    email: str,
    password: str,
    full_name: str,
    role: UserRole,
) -> User:
    normalized_email = email.lower()
    if session.exec(select(User).where(User.email == normalized_email)).first():
        raise ValueError("User already exists")
    user = User(
        email=normalized_email,
        password_hash=get_password_hash(password),
        full_name=full_name,
        role=role,
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


SESSION_USER_KEY = "session_user_id"
SESSION_TOKEN = "session_token"


def login_user(request: Request, user: User) -> None:
    request.session[SESSION_USER_KEY] = user.id
    request.session[SESSION_TOKEN] = secrets.token_urlsafe(16)


def logout_user(request: Request) -> None:
    request.session.pop(SESSION_USER_KEY, None)
    request.session.pop(SESSION_TOKEN, None)


def get_current_user(request: Request, session: Session = Depends(get_session)) -> User:
    user_id = request.session.get(SESSION_USER_KEY)
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    user = session.get(User, user_id)
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    return user


def get_optional_user(request: Request, session: Session = Depends(get_session)) -> User | None:
    user_id = request.session.get(SESSION_USER_KEY)
    if not user_id:
        return None
    user = session.get(User, user_id)
    if not user or not user.is_active:
        return None
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != UserRole.admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
    return user


def ensure_upload_access(upload_id: int, user: User, session: Session) -> Upload:
    upload = session.get(Upload, upload_id)
    if not upload:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    if user.role != UserRole.admin and upload.status != UploadStatus.completed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
    return upload


def ensure_default_accounts(session: Session) -> None:
    defaults = [
        {
            "email": "Admin",
            "full_name": "Portal Administrator",
            "role": UserRole.admin,
            "password": "Admin",
        },
        {
            "email": "User",
            "full_name": "Portal Analyst",
            "role": UserRole.analyst,
            "password": "User",
        },
    ]

    updated = False
    for spec in defaults:
        normalized_email = spec["email"].lower()
        existing = session.exec(select(User).where(User.email == normalized_email)).first()
        if existing:
            if not verify_password(spec["password"], existing.password_hash):
                existing.password_hash = get_password_hash(spec["password"])
                updated = True
            continue
        create_user(
            session,
            email=normalized_email,
            password=spec["password"],
            full_name=spec["full_name"],
            role=spec["role"],
        )
        updated = True

    if updated:
        session.commit()
