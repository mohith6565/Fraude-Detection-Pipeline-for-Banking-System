from passlib.context import CryptContext
import json
import pyotp
from typing import Optional

from app.database import User, TwoFactorAuth, UserSettings

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def create_user(db, email: str, password: str, name: str, role: str = "user") -> Optional[User]:
    email = email.lower()
    existing = db.query(User).filter_by(email=email).first()
    if existing:
        return None

    hashed = hash_password(password)
    user = User(email=email, password=hashed, name=name, role=role)
    db.add(user)
    db.commit()
    db.refresh(user)

    # create default settings and 2fa rows
    settings = UserSettings(user_id=user.id)
    db.add(settings)

    twof = TwoFactorAuth(user_id=user.id, is_enabled=False)
    db.add(twof)
    db.commit()
    return user


def prepare_2fa_for_user(db, user: User):
    secret = pyotp.random_base32()
    backup_codes = [pyotp.random_base32()[:10] for _ in range(5)]
    twof = db.query(TwoFactorAuth).filter_by(user_id=user.id).first()
    if not twof:
        twof = TwoFactorAuth(
            user_id=user.id,
            totp_secret=secret,
            is_enabled=False,
            backup_codes=json.dumps(backup_codes),
        )
        db.add(twof)
    else:
        twof.totp_secret = secret
        twof.backup_codes = json.dumps(backup_codes)
        twof.is_enabled = False
    db.commit()
    return secret, backup_codes


def confirm_2fa_for_user(db, user: User, token: str) -> bool:
    twof = db.query(TwoFactorAuth).filter_by(user_id=user.id).first()
    if not twof or not twof.totp_secret:
        return False
    totp = pyotp.TOTP(twof.totp_secret)
    if totp.verify(token):
        twof.is_enabled = True
        db.commit()
        return True
    return False


def enable_2fa_for_user(db, user: User) -> str:
    secret = pyotp.random_base32()
    twof = db.query(TwoFactorAuth).filter_by(user_id=user.id).first()
    if not twof:
        twof = TwoFactorAuth(user_id=user.id, totp_secret=secret, is_enabled=True)
        db.add(twof)
    else:
        twof.totp_secret = secret
        twof.is_enabled = True
    db.commit()
    return secret


def disable_2fa_for_user(db, user: User) -> bool:
    twof = db.query(TwoFactorAuth).filter_by(user_id=user.id).first()
    if twof:
        twof.is_enabled = False
        db.commit()
    return True


def verify_totp_for_user(db, user: User, token: str) -> bool:
    twof = db.query(TwoFactorAuth).filter_by(user_id=user.id).first()
    if not twof or not twof.totp_secret or not twof.is_enabled:
        return False
    totp = pyotp.TOTP(twof.totp_secret)
    return totp.verify(token)
