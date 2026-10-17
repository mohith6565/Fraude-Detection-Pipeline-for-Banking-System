import base64
import io
import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

from fastapi import FastAPI, Request, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import (
    init_db,
    get_db_context,
    User,
    Transaction,
    DisputeTicket,
    UserSettings,
    TwoFactorAuth,
    AuditLog,
    FraudAlert,
)
from app import auth
from app import exchange
from app import notifications
from app import tasks
from app.enterprise_integration import (
    init_enterprise_services,
    publish_transaction_to_kafka,
    publish_fraud_alert_to_kafka,
    get_velocity_features,
    cache_user_session,
    get_user_session,
    get_enterprise_stats,
)
from app.velocity_engine import get_velocity_engine, VelocityMetrics
from app.ingestion_gateway import router as ingestion_router, trace_middleware
from app.mlops_engine import get_model_ensemble, ShadowModelResponse
from app.observability_tracing import (
    TracingConfig,
    TraceContext,
    get_trace_logger,
    record_transaction_end_to_end,
    record_ml_prediction,
    TimedOperation,
)
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
import pyotp
import qrcode
import asyncio
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "app" / "templates"
STATIC_DIR = BASE_DIR / "app" / "static"

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize database
    init_db()
    logger.info("[INFO] Database initialized successfully")
    
    # Initialize enterprise services
    init_enterprise_services()
    logger.info("[INFO] Enterprise services initialized")
    
    # Initialize velocity engine
    velocity_engine = get_velocity_engine()
    logger.info("[INFO] Velocity engine initialized")
    
    # Initialize tracing
    TracingConfig.initialize()
    logger.info("[INFO] OpenTelemetry tracing initialized")
    
    # Initialize ML model ensemble
    try:
        model_ensemble = get_model_ensemble()
        logger.info("[OK] ML model ensemble loaded")
        logger.info(f"[INFO] Model stats: {model_ensemble.get_model_stats()}")
    except Exception as e:
        logger.warning(f"[WARN] Could not load ML models: {e}")
    
    seed_startup_data()
    yield

app = FastAPI(title="Fraud Atlas", version="2.0.0-enterprise", lifespan=lifespan)

# Add middleware for trace ID injection
app.middleware("http")(trace_middleware)

# Mount static files
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Include ingestion gateway routes
app.include_router(ingestion_router)

sessions = {}
broadcast_clients = set()


def format_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_inr(amount: float) -> str:
    """Format a number using the Indian numbering system with the rupee symbol.

    Examples:
      1234567.89 -> ₹12,34,567.89
    """
    try:
        n = float(amount)
    except Exception:
        return str(amount)

    sign = "" if n >= 0 else "-"
    n = abs(n)
    whole = int(n)
    frac = int(round((n - whole) * 100))

    # Indian grouping: last three, then groups of two
    s = str(whole)
    if len(s) > 3:
        last3 = s[-3:]
        rem = s[:-3]
        parts = []
        while len(rem) > 2:
            parts.insert(0, rem[-2:])
            rem = rem[:-2]
        if rem:
            parts.insert(0, rem)
        grouped = ",".join(parts + [last3])
    else:
        grouped = s

    return f"{sign}₹{grouped}.{frac:02d}"


def format_usd(amount: float) -> str:
    try:
        n = float(amount)
    except Exception:
        return str(amount)
    sign = "" if n >= 0 else "-"
    n = abs(n)
    whole = int(n)
    frac = int(round((n - whole) * 100))
    grouped = f"{whole:,}"
    return f"{sign}${grouped}.{frac:02d}"


def format_currency(amount: float, currency: str) -> str:
    if not currency:
        currency = "INR"
    if currency.upper() == "USD":
        return format_usd(amount)
    return format_inr(amount)


def _random_item(seq):
    return seq[random.randint(0, len(seq) - 1)]


def _amount_for_merchant(merchant: str) -> float:
    buckets = {
        "ATM Withdrawal": [40, 80, 120, 200, 320],
        "Airline Ticket": [180, 260, 410, 650, 980],
        "Hotel Booking": [120, 220, 310, 450, 760],
        "Luxury Retail": [220, 340, 620, 950, 1450],
        "Electronics Store": [90, 180, 260, 520, 899],
    }
    pool = buckets.get(merchant, [8, 14, 22, 35, 49, 68])
    base = _random_item(pool)
    spread = max(2, round(base * 0.16))
    return round(base + (random.random() * spread * 2 - spread), 2)


def _make_live_transaction(force_fraud: bool = False) -> dict:
    merchants = [
        "Starbucks",
        "Target",
        "Electronics Store",
        "ATM Withdrawal",
        "Ride Share",
        "Grocery Market",
        "Online Marketplace",
        "Pharmacy",
        "Hotel Booking",
        "Airline Ticket",
        "Gas Station",
        "Luxury Retail",
    ]
    locations = [
        "New York, US",
        "Austin, US",
        "Chicago, US",
        "San Francisco, US",
        "Miami, US",
        "Seattle, US",
        "London, UK",
        "Toronto, CA",
        "Berlin, DE",
        "Singapore, SG",
    ]
    cards = [
        "Visa **** 4821",
        "Mastercard **** 1024",
        "Amex **** 9302",
        "Visa **** 7748",
        "Discover **** 6642",
        "Mastercard **** 5519",
    ]

    merchant = _random_item(merchants)
    card = _random_item(cards)
    location = _random_item(locations)
    amount = _amount_for_merchant(merchant)
    if force_fraud:
        amount = round(amount * (1.4 + random.random() * 1.4), 2)

    # simple scoring heuristic ported from client
    score = 12 + random.random() * 18
    if amount > 400:
        score += 22
    elif amount > 180:
        score += 12
    elif amount < 12:
        score += 5

    if any(k in merchant for k in ["ATM", "Luxury", "Airline", "Hotel"]):
        score += 10

    if force_fraud:
        score = 90 + random.random() * 10

    score = max(0, min(100, round(score)))
    shadow_score = max(0, min(100, score + int(random.random() * 14 - 7)))

    tx = {
        "id": f"TX-{uuid.uuid4().hex[:8].upper()}",
        "merchant": merchant,
        "amount": amount,
        "location": location,
        "card": card,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "score": score,
        "shadowScore": shadow_score,
    }
    return tx


async def broadcast_attack(count: int = 5):
    if not broadcast_clients:
        return

    attack_events = []
    for _ in range(count):
        tx = _make_live_transaction(force_fraud=True)
        tx["attack"] = True
        attack_events.append(tx)

    for queue in list(broadcast_clients):
        for event in attack_events:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass


@app.get("/events/transactions")
async def transactions_events():
    """Server-Sent-Events endpoint streaming live transaction events as JSON.

    This yields an event every second. Clients should use EventSource.
    """

    queue = asyncio.Queue(maxsize=20)
    broadcast_clients.add(queue)

    async def event_generator():
        try:
            while True:
                msg = None
                try:
                    msg = queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass

                if msg is not None:
                    payload = json.dumps(msg)
                    yield f"data: {payload}\n\n"
                    continue

                tx = _make_live_transaction()
                payload = json.dumps(tx)
                yield f"data: {payload}\n\n"
                await asyncio.sleep(1)
        finally:
            broadcast_clients.discard(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/admin/inject-attack")
async def api_admin_inject_attack(request: Request):
    if not is_authenticated(request) or not is_admin(request):
        return JSONResponse({"success": False, "error": "auth required"}, status_code=401)

    data = await request.json()
    count = int(data.get("count", 5)) if isinstance(data, dict) else 5
    await broadcast_attack(count)
    return {"success": True, "message": f"Broadcasted {count} attack transactions."}


def serialize_transaction(tx: Transaction, currency: str = "INR") -> dict:
    amount_val = float(tx.amount or 0.0)
    display_amount = amount_val
    try:
        if currency and currency.upper() != "INR":
            display_amount = exchange.convert_amount(amount_val, "INR", currency.upper())
    except Exception:
        display_amount = amount_val

    return {
        "id": tx.transaction_id,
        "merchant": tx.merchant,
        "amount": display_amount,
        "amount_formatted": format_currency(display_amount, currency),
        "location": tx.location,
        "timestamp": format_datetime(tx.timestamp),
        "status": tx.status,
        "fraud_score": int(tx.fraud_score or 0),
    }


def serialize_alert(db, alert: FraudAlert) -> dict:
    user_email = "System"
    if alert.user_id:
        user = db.get(User, alert.user_id)
        if user:
            user_email = user.email
    return {
        "id": alert.alert_id,
        "type": alert.fraud_type,
        "severity": alert.severity,
        "user_email": user_email,
        "timestamp": format_datetime(alert.timestamp),
        "details": alert.details,
    }


def serialize_dispute(db, ticket: DisputeTicket) -> dict:
    transaction = ticket.transaction
    # Determine user's currency preference if available
    user_currency = "INR"
    try:
        if ticket.user and ticket.user.settings:
            user_currency = ticket.user.settings.currency or "INR"
    except Exception:
        user_currency = "INR"

    # convert transaction amount if needed
    try:
        raw_amount = float(transaction.amount) if transaction else 0.0
        if user_currency and user_currency.upper() != "INR":
            display_amount = exchange.convert_amount(raw_amount, "INR", user_currency.upper())
        else:
            display_amount = raw_amount
    except Exception:
        display_amount = transaction.amount if transaction else 0.0

    return {
        "ticket_id": ticket.ticket_id,
        "transaction_id": transaction.transaction_id if transaction else "Unknown",
        "merchant": transaction.merchant if transaction else "Unknown",
        "amount": format_currency(display_amount, user_currency),
        "status": ticket.status.title(),
        "reason": ticket.reason.replace("_", " ").title(),
        "description": ticket.description,
        "resolution": ticket.resolution,
        "timestamp": format_datetime(ticket.created_at),
        "updated_at": format_datetime(ticket.updated_at) if ticket.updated_at else None,
    }


def create_session(email: str, role: str) -> str:
    session_id = str(uuid.uuid4())
    sessions[session_id] = {"email": email, "role": role, "created": datetime.now()}
    return session_id


def get_session(request: Request) -> Optional[dict]:
    session_id = request.cookies.get("session_id")
    if session_id and session_id in sessions:
        return sessions[session_id]
    return None


def get_current_user(request: Request) -> Optional[User]:
    session = get_session(request)
    if not session:
        return None
    with get_db_context() as db:
        user = db.query(User).filter_by(email=session["email"]).first()
        if not user:
            session_id = request.cookies.get("session_id")
            if session_id in sessions:
                del sessions[session_id]
            return None
        return user


def is_authenticated(request: Request) -> bool:
    return get_current_user(request) is not None


def is_admin(request: Request) -> bool:
    session = get_session(request)
    return bool(session and session.get("role") == "admin")


def generate_qr_data_uri(secret: str, email: str) -> str:
    totp = pyotp.TOTP(secret)
    uri = totp.provisioning_uri(name=email, issuer_name="Fraud Atlas")
    qr = qrcode.QRCode(box_size=6, border=2)
    qr.add_data(uri)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode()
    return f"data:image/png;base64,{encoded}"


def get_user_transactions_from_db(db, user_id: int, limit: int = 20) -> list:
    transactions = (
        db.query(Transaction)
        .filter_by(user_id=user_id)
        .order_by(Transaction.timestamp.desc())
        .limit(limit)
        .all()
    )
    # fetch user currency preference
    try:
        settings = db.query(UserSettings).filter_by(user_id=user_id).first()
        currency = settings.currency if settings and settings.currency else "INR"
    except Exception:
        currency = "INR"
    return [serialize_transaction(tx, currency=currency) for tx in transactions]


def get_user_disputes_from_db(db, user_id: int) -> list:
    disputes = (
        db.query(DisputeTicket)
        .filter_by(user_id=user_id)
        .order_by(DisputeTicket.created_at.desc())
        .all()
    )
    return [serialize_dispute(db, ticket) for ticket in disputes]


def get_recent_fraud_alerts(db, limit: int = 12) -> list:
    alerts = db.query(FraudAlert).order_by(FraudAlert.timestamp.desc()).limit(limit).all()
    return [serialize_alert(db, alert) for alert in alerts]


def get_user_settings(db, user_id: int) -> UserSettings:
    settings = db.query(UserSettings).filter_by(user_id=user_id).first()
    if not settings:
        settings = UserSettings(user_id=user_id)
        db.add(settings)
        db.commit()
        db.refresh(settings)
    return settings


def get_last_login(db, user: User) -> Optional[str]:
    entry = (
        db.query(AuditLog)
        .filter(AuditLog.resource == user.email, AuditLog.action == "login")
        .order_by(AuditLog.timestamp.desc())
        .first()
    )
    return format_datetime(entry.timestamp) if entry else None


def create_audit_log(db, admin_id: Optional[int], action: str, resource: str, details: str, ip_address: Optional[str] = None):
    log = AuditLog(
        admin_id=admin_id,
        action=action,
        resource=resource,
        details=details,
        ip_address=ip_address,
    )
    db.add(log)
    db.commit()


def create_demo_transactions(db, user: User, total: int = 15):
    current_count = db.query(Transaction).filter_by(user_id=user.id).count()
    if current_count >= total:
        return

    merchants = [
        "Starbucks",
        "Target",
        "Electronics Store",
        "Airline Ticket",
        "Hotel Booking",
        "Grocery Market",
        "Online Marketplace",
        "Luxury Retail",
        "Pharmacy",
        "Ride Share",
    ]
    locations = [
        "New York, US",
        "San Francisco, US",
        "Chicago, US",
        "London, UK",
        "Toronto, CA",
        "Berlin, DE",
        "Austin, US",
    ]

    while current_count < total:
        amount = round(random.uniform(12, 980), 2)
        score = round(random.uniform(5, 98), 0)
        transaction = Transaction(
            user_id=user.id,
            transaction_id=f"TX-{uuid.uuid4().hex[:8].upper()}",
            merchant=random.choice(merchants),
            card_masked=f"Visa **** {random.randint(1000, 9999)}",
            location=random.choice(locations),
            amount=amount,
            fraud_score=score,
            status="Flagged" if score > 70 else "Approved",
            timestamp=datetime.now() - timedelta(hours=random.randint(1, 72)),
        )
        db.add(transaction)
        current_count += 1

    db.commit()


def create_demo_alerts(db, limit: int = 12):
    if db.query(FraudAlert).count() > 0:
        return

    users = db.query(User).filter(User.role == "user").all()
    fraud_types = [
        "Card-Not-Present",
        "Account Takeover",
        "Synthetic Identity",
        "Refund Abuse",
        "Mule Transfer",
    ]
    severities = ["Low", "Medium", "High"]

    for i in range(limit):
        user = random.choice(users) if users else None
        alert = FraudAlert(
            alert_id=f"ALT-{5001 + i}",
            user_id=user.id if user else None,
            fraud_type=random.choice(fraud_types),
            severity=random.choice(severities),
            details=f"Suspicious transaction pattern detected for {user.email if user else 'an account'}.",
            timestamp=datetime.now() - timedelta(minutes=5 * i),
            resolved=False,
        )
        db.add(alert)
    db.commit()


def seed_startup_data():
    """Simplified startup data - skip demo transactions to avoid schema issues"""
    try:
        with get_db_context() as db:
            if not db.query(User).filter_by(email="user@example.com").first():
                auth.create_user(db, "user@example.com", "password123", "John Doe", role="user")
            if not db.query(User).filter_by(email="admin@example.com").first():
                auth.create_user(db, "admin@example.com", "admin123", "Admin User", role="admin")
    except Exception as e:
        logger.warning(f"[WARN] Could not seed startup data: {e}")


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request) -> HTMLResponse:
    if is_authenticated(request):
        return RedirectResponse(url="/dashboard", status_code=302)
    return templates.TemplateResponse(
        request=request,
        name="auth/login.html",
        context={"page_title": "Sign In - Fraud Atlas"},
    )


@app.get("/signup", response_class=HTMLResponse)
def signup_page(request: Request) -> HTMLResponse:
    if is_authenticated(request):
        return RedirectResponse(url="/dashboard", status_code=302)
    return templates.TemplateResponse(
        request=request,
        name="auth/signup.html",
        context={"page_title": "Create Account - Fraud Atlas"},
    )

@app.post("/api/auth/login")
async def api_login(request: Request):
    data = await request.json()
    email = data.get("email", "").lower()
    password = data.get("password", "")
    totp_token = data.get("totp")

    with get_db_context() as db:
        user = db.query(User).filter_by(email=email).first()
        if not user:
            return {"success": False, "error": "Invalid credentials"}

        if not auth.verify_password(password, user.password):
            return {"success": False, "error": "Invalid credentials"}

        twof = db.query(TwoFactorAuth).filter_by(user_id=user.id).first()
        if twof and twof.is_enabled:
            if not totp_token or not auth.verify_totp_for_user(db, user, totp_token):
                return {"success": False, "error": "Two-factor authentication required or invalid token"}

        session_id = create_session(email, user.role)
        create_audit_log(db, user.id, "login", user.email, "User signed in")
        return {"success": True, "session_id": session_id, "redirect": "/dashboard"}


@app.post("/api/auth/signup")
async def api_signup(request: Request):
    data = await request.json()
    email = data.get("email", "").lower()
    password = data.get("password", "")
    name = data.get("name", "")

    with get_db_context() as db:
        existing = db.query(User).filter_by(email=email).first()
        if existing:
            return {"success": False, "error": "Email already registered"}

        user = auth.create_user(db, email, password, name, role="user")
        if not user:
            return {"success": False, "error": "Could not create user"}

        create_demo_transactions(db, user, total=12)
        create_audit_log(db, user.id, "signup", user.email, "User account created")
        session_id = create_session(email, "user")
        return {"success": True, "session_id": session_id, "redirect": "/dashboard"}


@app.get("/logout")
def logout(request: Request):
    user = get_current_user(request)
    if user:
        with get_db_context() as db:
            create_audit_log(db, user.id, "logout", user.email, "User signed out")

    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("session_id")
    session_id = request.cookies.get("session_id")
    if session_id in sessions:
        del sessions[session_id]
    return response


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    if user.role == "admin":
        return RedirectResponse(url="/admin", status_code=302)

    with get_db_context() as db:
        transactions = get_user_transactions_from_db(db, user.id, limit=15)

    flagged_count = sum(1 for tx in transactions if tx["status"] == "Flagged")
    approved_count = sum(1 for tx in transactions if tx["status"] == "Approved")
    avg_score = round(sum(tx["fraud_score"] for tx in transactions) / len(transactions), 1) if transactions else 0
    approval_rate = round((approved_count / len(transactions)) * 100, 1) if transactions else 0

    return templates.TemplateResponse(
        request=request,
        name="dashboard/user-dashboard.html",
        context={
            "page_title": "My Dashboard - Fraud Atlas",
            "user_email": user.email,
            "transactions": transactions,
            "flagged_count": flagged_count,
            "avg_score": avg_score,
            "approval_rate": approval_rate,
        },
    )


@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard(request: Request) -> HTMLResponse:
    if not is_authenticated(request) or not is_admin(request):
        return RedirectResponse(url="/login", status_code=302)

    user = get_current_user(request)
    with get_db_context() as db:
        total_transactions = db.query(Transaction).count()
        fraud_detected = db.query(Transaction).filter(Transaction.status == "Flagged").count()
        detection_rate = round((fraud_detected / total_transactions) * 100, 1) if total_transactions else 0.0
        false_positive_rate = round(max(0.0, min(12.0, fraud_detected * 0.08)), 1)
        alerts = get_recent_fraud_alerts(db, 12)

    return templates.TemplateResponse(
        request=request,
        name="dashboard/admin-dashboard.html",
        context={
            "page_title": "Admin Dashboard - Fraud Atlas",
            "admin_email": user.email,
            "total_transactions": total_transactions,
            "fraud_detected": fraud_detected,
            "detection_rate": detection_rate,
            "false_positive_rate": false_positive_rate,
            "model_precision": 92.1,
            "model_recall": 88.7,
            "avg_latency_ms": 42,
            "uptime": "99.98%",
            "global_threshold": 75,
            "alerts": alerts,
        },
    )


@app.get("/api/admin/disputes")
def api_admin_disputes(request: Request):
    if not is_authenticated(request) or not is_admin(request):
        return JSONResponse({"success": False, "error": "auth required"}, status_code=401)
    with get_db_context() as db:
        tickets = db.query(DisputeTicket).order_by(DisputeTicket.created_at.desc()).all()
        disputes = [serialize_dispute(db, t) for t in tickets]
    return {"success": True, "disputes": disputes}


@app.post("/api/admin/resolve-dispute")
async def api_admin_resolve(request: Request):
    if not is_authenticated(request) or not is_admin(request):
        return JSONResponse({"success": False, "error": "auth required"}, status_code=401)
    data = await request.json()
    ticket_id = data.get("ticket_id")
    resolution = data.get("resolution", "Resolved")
    with get_db_context() as db:
        ticket = db.query(DisputeTicket).filter_by(ticket_id=ticket_id).first()
        if not ticket:
            return {"success": False, "error": "not found"}
        ticket.status = "resolved"
        ticket.resolution = resolution
        db.commit()
        # notify user
        try:
            user = ticket.user
            settings = user.settings if user else None
            if user and settings and settings.notification_email:
                notifications.send_email_async(user.email, f"Dispute {ticket.ticket_id} resolved", resolution)
        except Exception:
            pass
    return {"success": True}


# Prometheus metrics endpoint
REQUEST_COUNT = Counter('fraud_atlas_requests_total', 'Total HTTP requests')


@app.get('/health')
async def health_check():
    """Health check endpoint for Kubernetes and load balancers"""
    try:
        # Check database
        with get_db_context() as db:
            db.query(User).limit(1).first()
        
        # Get enterprise service stats
        stats = get_enterprise_stats()
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "services": stats,
        }
    except Exception as e:
        logger.error(f"[ERROR] Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
        }, 503


@app.get('/metrics')
def metrics_endpoint():
    return generate_latest(), 200, { 'Content-Type': CONTENT_TYPE_LATEST }


@app.get("/", response_class=HTMLResponse)
def serve_home(request: Request) -> HTMLResponse:
    if is_authenticated(request):
        return RedirectResponse(url="/dashboard", status_code=302)
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"page_title": "Fraud Atlas | Real-Time Fraud Detection"},
    )


# ====== NEW ROUTES ======

# Analytics Endpoints
@app.get("/api/analytics/detection-rate")
def get_detection_rate():
    """Get fraud detection rate trends"""
    with get_db_context() as db:
        today = datetime.now().date()
        dates = []
        rates = []
        for offset in range(6, -1, -1):
            day = today - timedelta(days=offset)
            start = datetime(day.year, day.month, day.day)
            end = start + timedelta(days=1)
            total = db.query(Transaction).filter(Transaction.timestamp >= start, Transaction.timestamp < end).count()
            flagged = db.query(Transaction).filter(Transaction.timestamp >= start, Transaction.timestamp < end, Transaction.status == "Flagged").count()
            dates.append(day.strftime("%a"))
            rates.append(round((flagged / total) * 100, 1) if total else 0.0)
        return {"dates": dates, "rates": rates}

@app.get("/api/analytics/fraud-by-type")
def get_fraud_by_type():
    """Get fraud distribution by type"""
    with get_db_context() as db:
        counts = {}
        for alert in db.query(FraudAlert).all():
            counts[alert.fraud_type] = counts.get(alert.fraud_type, 0) + 1

        fraud_types = [
            "Card-Not-Present",
            "Account Takeover",
            "Synthetic ID",
            "Refund Abuse",
            "Mule Transfer",
        ]
        return {
            "types": fraud_types,
            "counts": [counts.get(fraud_type, 0) for fraud_type in fraud_types],
        }

# Settings Endpoints
@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request) -> HTMLResponse:
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    with get_db_context() as db:
        settings = get_user_settings(db, user.id)
        twof = db.query(TwoFactorAuth).filter_by(user_id=user.id).first()
        backup_codes = []
        if twof and twof.backup_codes:
            try:
                backup_codes = json.loads(twof.backup_codes)
            except json.JSONDecodeError:
                backup_codes = []
        last_login = get_last_login(db, user) or "Not available"

    return templates.TemplateResponse(
        request=request,
        name="settings.html",
        context={
            "page_title": "Settings - Fraud Atlas",
            "user_email": user.email,
            "created_at": format_datetime(user.created_at),
            "last_login": last_login,
            "settings": settings,
            "twofa_enabled": bool(twof and twof.is_enabled),
            "backup_codes": backup_codes,
        },
    )


@app.get("/disputes", response_class=HTMLResponse)
def disputes_page(request: Request) -> HTMLResponse:
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    with get_db_context() as db:
        transactions = get_user_transactions_from_db(db, user.id, limit=12)
        disputes = get_user_disputes_from_db(db, user.id)

    return templates.TemplateResponse(
        request=request,
        name="disputes.html",
        context={
            "page_title": "Disputes - Fraud Atlas",
            "user_email": user.email,
            "transactions": transactions,
            "disputes": disputes,
        },
    )


@app.get("/dashboard/admin-disputes.html", response_class=HTMLResponse)
def admin_disputes_page(request: Request) -> HTMLResponse:
    if not is_authenticated(request) or not is_admin(request):
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse(
        request=request,
        name="dashboard/admin-disputes.html",
        context={"page_title": "Admin Disputes - Fraud Atlas"},
    )


@app.get("/analytics", response_class=HTMLResponse)
def analytics_page(request: Request) -> HTMLResponse:
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    with get_db_context() as db:
        total_transactions = db.query(Transaction).count()
        fraud_detected = db.query(Transaction).filter(Transaction.status == "Flagged").count()
        detection_rate = round((fraud_detected / total_transactions) * 100, 1) if total_transactions else 0.0
        fraud_type_counts = {
            "Card-Not-Present": 0,
            "Account Takeover": 0,
            "Synthetic ID": 0,
            "Refund Abuse": 0,
            "Mule Transfer": 0,
        }
        for alert in db.query(FraudAlert).all():
            fraud_type_counts[alert.fraud_type] = fraud_type_counts.get(alert.fraud_type, 0) + 1

        today = datetime.now().date()
        trend_labels = []
        trend_rates = []
        for offset in range(6, -1, -1):
            day = today - timedelta(days=offset)
            start = datetime(day.year, day.month, day.day)
            end = start + timedelta(days=1)
            daily_total = (
                db.query(Transaction)
                .filter(Transaction.timestamp >= start, Transaction.timestamp < end)
                .count()
            )
            daily_flagged = (
                db.query(Transaction)
                .filter(Transaction.timestamp >= start, Transaction.timestamp < end, Transaction.status == "Flagged")
                .count()
            )
            trend_labels.append(day.strftime("%a"))
            trend_rates.append(round((daily_flagged / daily_total) * 100, 1) if daily_total else 0.0)

    return templates.TemplateResponse(
        request=request,
        name="analytics.html",
        context={
            "page_title": "Analytics - Fraud Atlas",
            "total_transactions": total_transactions,
            "fraud_detected": fraud_detected,
            "detection_rate": detection_rate,
            "false_positive_rate": round((fraud_detected / total_transactions) * 4, 1) if total_transactions else 0.0,
            "avg_latency_ms": 42,
            "model_precision": 92.1,
            "model_recall": 88.7,
            "uptime": "99.98%",
            "fraud_type_labels": list(fraud_type_counts.keys()),
            "fraud_type_counts": list(fraud_type_counts.values()),
            "trend_labels": trend_labels,
            "trend_rates": trend_rates,
        },
    )


@app.get("/2fa/setup", response_class=HTMLResponse)
def two_factor_setup(request: Request) -> HTMLResponse:
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    with get_db_context() as db:
        twof = db.query(TwoFactorAuth).filter_by(user_id=user.id).first()
        if not twof or not twof.totp_secret:
            secret, backup_codes = auth.prepare_2fa_for_user(db, user)
        else:
            secret = twof.totp_secret
            try:
                backup_codes = json.loads(twof.backup_codes or "[]")
            except json.JSONDecodeError:
                backup_codes = []
        qr_image = generate_qr_data_uri(secret, user.email)
        twofa_enabled = twof.is_enabled if twof else False

    return templates.TemplateResponse(
        request=request,
        name="2fa-setup.html",
        context={
            "page_title": "Two-Factor Authentication - Fraud Atlas",
            "user_email": user.email,
            "qr_image": qr_image,
            "secret_key": secret,
            "twofa_enabled": twofa_enabled,
            "backup_codes": backup_codes,
        },
    )


@app.post("/api/auth/2fa/confirm")
async def api_confirm_2fa(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Authentication required"}, status_code=401)

    data = await request.json()
    token = data.get("token", "")

    with get_db_context() as db:
        if not auth.confirm_2fa_for_user(db, user, token):
            return {"success": False, "error": "Invalid verification code"}
        create_audit_log(db, user.id, "2fa_enable", user.email, "Two-factor authentication enabled")
        twof = db.query(TwoFactorAuth).filter_by(user_id=user.id).first()
        backup_codes = []
        try:
            backup_codes = json.loads(twof.backup_codes or "[]")
        except json.JSONDecodeError:
            backup_codes = []
        return {"success": True, "backup_codes": backup_codes}


@app.post("/api/auth/2fa/disable")
async def api_disable_2fa(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Authentication required"}, status_code=401)

    with get_db_context() as db:
        auth.disable_2fa_for_user(db, user)
        create_audit_log(db, user.id, "2fa_disable", user.email, "Two-factor authentication disabled")
        return {"success": True}


@app.post("/api/change-password")
async def change_password(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Authentication required"}, status_code=401)

    data = await request.json()
    old_password = data.get("old_password", "")
    new_password = data.get("new_password", "")

    with get_db_context() as db:
        user_record = db.query(User).filter_by(email=user.email).first()
        if not auth.verify_password(old_password, user_record.password):
            return {"success": False, "error": "Current password is incorrect"}

        user_record.password = auth.hash_password(new_password)
        db.commit()
        create_audit_log(db, user.id, "password_change", user.email, "User changed password")
        return {"success": True}


@app.post("/api/update-preferences")
async def update_preferences(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Authentication required"}, status_code=401)

    data = await request.json()

    with get_db_context() as db:
        settings = get_user_settings(db, user.id)
        settings.fraud_threshold = float(data.get("fraud_threshold", settings.fraud_threshold))
        settings.alert_frequency = data.get("alert_frequency", settings.alert_frequency)
        settings.notification_email = bool(data.get("notification_email", settings.notification_email))
        settings.notification_sms = bool(data.get("notification_sms", settings.notification_sms))
        settings.phone_number = data.get("phone_number")
        settings.currency = data.get("currency", settings.currency)
        db.commit()
        create_audit_log(db, user.id, "preferences_update", user.email, "Updated fraud and alert preferences")
        return {"success": True}


@app.post("/api/file-dispute")
async def file_dispute(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Authentication required"}, status_code=401)

    data = await request.json()
    transaction_id = data.get("transaction_id")
    reason = data.get("reason")
    description = data.get("description")

    with get_db_context() as db:
        transaction = db.query(Transaction).filter_by(user_id=user.id, transaction_id=transaction_id).first()
        if not transaction:
            return {"success": False, "error": "Transaction not found"}

        ticket = DisputeTicket(
            ticket_id=f"DIS-{uuid.uuid4().hex[:8].upper()}",
            user_id=user.id,
            transaction_id=transaction.id,
            reason=reason,
            description=description,
            status="pending",
        )
        db.add(ticket)
        db.commit()
        create_audit_log(db, user.id, "dispute_created", transaction.transaction_id, f"Filed dispute for {transaction.transaction_id}")
        # send notification asynchronously
        try:
            settings = get_user_settings(db, user.id)
            if settings.notification_email and settings.phone_number:
                notifications.send_email_async(user.email, "Dispute filed", f"Your dispute {ticket.ticket_id} was filed for transaction {transaction.transaction_id}.")
                notifications.send_sms_async(settings.phone_number, f"Dispute {ticket.ticket_id} filed")
            elif settings.notification_email:
                notifications.send_email_async(user.email, "Dispute filed", f"Your dispute {ticket.ticket_id} was filed for transaction {transaction.transaction_id}.")
        except Exception:
            pass

        return {"success": True}


@app.post("/api/update-dispute")
async def update_dispute(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Authentication required"}, status_code=401)

    data = await request.json()
    ticket_id = data.get("ticket_id")
    resolution = data.get("resolution")

    with get_db_context() as db:
        ticket = db.query(DisputeTicket).filter_by(ticket_id=ticket_id, user_id=user.id).first()
        if not ticket:
            return {"success": False, "error": "Dispute ticket not found"}

        ticket.resolution = resolution or "Additional user update submitted"
        ticket.status = "reviewing" if ticket.status != "resolved" else ticket.status
        db.commit()
        create_audit_log(db, user.id, "dispute_updated", ticket.ticket_id, "User updated dispute case")
        return {"success": True}


@app.post("/api/delete-account")
async def delete_account(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Authentication required"}, status_code=401)

    with get_db_context() as db:
        db.query(DisputeTicket).filter_by(user_id=user.id).delete()
        db.query(Transaction).filter_by(user_id=user.id).delete()
        db.query(UserSettings).filter_by(user_id=user.id).delete()
        db.query(TwoFactorAuth).filter_by(user_id=user.id).delete()
        account = db.query(User).filter_by(id=user.id).first()
        if account:
            db.delete(account)
            db.commit()

    session_id = request.cookies.get("session_id")
    if session_id and session_id in sessions:
        del sessions[session_id]
    response = JSONResponse({"success": True})
    response.delete_cookie("session_id")
    return response


@app.get("/api/metrics")
def metrics() -> dict:
    with get_db_context() as db:
        total_transactions = db.query(Transaction).count()
        fraud_detected = db.query(Transaction).filter(Transaction.status == "Flagged").count()
        return {
            "total_transactions": total_transactions,
            "fraud_detected": fraud_detected,
            "fraud_rate": round((fraud_detected / total_transactions) * 100, 1) if total_transactions else 0.0,
        }


@app.get("/api/overview")
def overview() -> dict:
    return {
        "stack": [
            {"name": "Apache Kafka", "role": "ingestion firehose", "detail": "Decouples producers from downstream scoring and absorbs traffic spikes."},
            {"name": "Spark Streaming", "role": "feature engineering", "detail": "Maintains rolling counts, velocity windows, and geo-distance features."},
            {"name": "FastAPI", "role": "inference API", "detail": "Serves predictions with low overhead and clean async-ready routes."},
            {"name": "Redis", "role": "hot feature cache", "detail": "Stores recent activity for fast lookups during scoring."},
            {"name": "Prometheus + Evidently", "role": "monitoring", "detail": "Tracks latency, model drift, and score distribution shifts."},
            {"name": "Docker + Kubernetes", "role": "deployment", "detail": "Packages and scales the system in production."},
        ],
        "fraud_types": [
            "card_not_present",
            "account_takeover",
            "synthetic_identity",
            "refund_abuse",
            "mule_transfer",
        ],
        "latency_budget_ms": 100,
        "deployment": "docker_kubernetes",
    }


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)