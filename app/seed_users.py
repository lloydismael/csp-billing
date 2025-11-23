from __future__ import annotations

import argparse

from sqlmodel import Session, select

from app.auth import create_user
from app.database import init_db, session_scope
from app.models import User, UserRole


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed a user into the CSP billing portal database")
    parser.add_argument("--email", required=True, help="User email")
    parser.add_argument("--password", required=True, help="Plaintext password")
    parser.add_argument("--name", required=True, help="Full name")
    parser.add_argument(
        "--role",
        choices=[role.value for role in UserRole],
        default=UserRole.analyst.value,
        help="User role",
    )
    return parser.parse_args()


def user_exists(session: Session, email: str) -> bool:
    statement = select(User).where(User.email == email.lower())
    return session.exec(statement).first() is not None


def main() -> None:
    args = parse_args()
    init_db()
    with session_scope() as session:
        if user_exists(session, args.email):
            print(f"User {args.email} already exists. No changes made.")
            return
        user = create_user(
            session,
            email=args.email,
            password=args.password,
            full_name=args.name,
            role=UserRole(args.role),
        )
        print(f"Created user {user.email} with role {user.role.value}.")


if __name__ == "__main__":
    main()
