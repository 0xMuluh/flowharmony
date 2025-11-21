
from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from flask import Flask, abort, jsonify, redirect, render_template, request, send_from_directory, url_for

try:
    from redis import Redis
    from redis.exceptions import RedisError
except Exception:  # pragma: no cover - optional dependency
    Redis = None  # type: ignore
    RedisError = Exception  # type: ignore

app = Flask(__name__)


logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get("FLOWHARMONY_REDIS_URL")
REDIS_CLIENT = None
if REDIS_URL and Redis:
    try:
        REDIS_CLIENT = Redis.from_url(REDIS_URL, decode_responses=True)
        REDIS_CLIENT.ping()
    except Exception as exc:  # pragma: no cover - best effort logging
        logger.warning("Redis connection failed for %s: %s", REDIS_URL, exc)
        REDIS_CLIENT = None
elif REDIS_URL and not Redis:
    logger.warning("FLOWHARMONY_REDIS_URL is set but redis client is unavailable")

# --- Domain models ---


@dataclass
class SiteConfig:
    site_id: str
    name: str
    lunch_window_start: time
    lunch_window_end: time
    wave_duration_minutes: int
    portion_grams: int
    pan_capacity_portions: int
    dish_name: str


@dataclass
class WaveState:
    index: int
    start_time: time
    end_time: time
    expected_diners_pattern: int
    predicted_diners: Optional[int] = None
    actual_diners: Optional[int] = None


@dataclass
class DecisionCard:
    site_id: str
    wave_index: int
    timestamp: datetime
    predicted_diners_next_wave: int
    suggested_grams: int
    pan_fill_percent: int
    satisfaction_percent: Optional[int]
    dish_name: str
    dish_type: str
    suggested_portions: float
    note: str
    service_slot: str
    slot_label: str
    station_label: Optional[str] = None
    status: str = "pending"


@dataclass
class Dish:
    dish_id: str
    name: str
    type: str
    avg_portion_grams: int
    co2_per_kg: float
    cost_per_kg: float


@dataclass
class MenuDay:
    date: date
    weather: Optional[str]
    predicted_covers: int
    main_dish_planned: Dish
    recommended_dish: Dish
    service_slot: str
    slot_label: str
    station: Optional[str]
    uptake_if_kept: float
    suggested_swap_savings_kg: float
    suggested_swap_savings_eur: float
    swap_status: str = "suggested"
    additional_dishes: List[Dish] = field(default_factory=list)


@dataclass
class ServiceDay:
    site_id: str
    date: date
    total_expected_diners: int
    diners_so_far: int = 0
    wave_index: int = 0
    pan_fill_percent: int = 60
    feedback_scores: List[int] = field(default_factory=list)
    detailed_feedback: List[Dict[str, object]] = field(default_factory=list)
    reaction_stream: List[Dict[str, object]] = field(default_factory=list)
    last_decision: Optional[DecisionCard] = None
    waves: List[WaveState] = field(default_factory=list)
    predicted_waste_if_no_action_kg: float = 0.0
    predicted_waste_with_flowharmony_kg: float = 0.0
    predicted_waste_savings_eur: float = 0.0
    actual_waste_kg: Optional[float] = None
    actual_waste_baseline_kg: Optional[float] = None
    actual_co2_saved_kg: Optional[float] = None
    actual_money_saved_eur: Optional[float] = None


@dataclass
class WeeklySummary:
    site_id: str
    week_start: date
    week_end: date
    waste_kg: float
    waste_baseline_kg: float
    co2_saved_kg: float
    money_saved_eur: float
    bonus_trigger_met: bool
    bonus_amount_eur: float


@dataclass
class TodayBanner:
    date_str: str
    predicted_covers: int
    predicted_covers_delta_pct: float
    predicted_waste_no_action_kg: float
    predicted_waste_no_action_eur: float
    predicted_waste_with_fh_kg: float
    predicted_waste_with_fh_eur: float


@dataclass
class MenuPreventionRow:
    date_str: str
    date_iso: str
    weather: str
    predicted_covers: int
    recommended_dish_name: str
    current_dish_name: str
    service_slot: str
    slot_label: str
    station_label: str
    uptake_if_kept_pct: int
    suggested_swap_text: str
    swap_status: str
    other_items: str


@dataclass
class AlertItem:
    date_str: str
    date_iso: str
    message: str
    action_text: str
    from_dish_name: str
    to_dish_name: str
    service_slot: str
    slot_label: str


@dataclass
class LiveDishRow:
    dish_name: str
    planned_kg: float
    sold_kg: float
    forecast_next_30_min_kg: float
    pan_level_percent: int
    satisfaction_percent: int


@dataclass
class ManagerViewModel:
    today_banner: TodayBanner
    menu_prevention_rows: List[MenuPreventionRow]
    alerts_48h: List[AlertItem]
    live_overview_rows: List[LiveDishRow]
    weekly_report: WeeklySummary


@dataclass
class WaitLineView:
    service_slot: str
    slot_label: str
    station: str
    wait_minutes: float
    line_length: int
    status: str
    message: str


@dataclass
class FeedbackSummaryRow:
    key: str
    label: str
    total_responses: int
    average_score: Optional[float]
    top_response: Optional[str]
    response_breakdown: Dict[str, int]


@dataclass
class RedirectAlert:
    title: str
    subtitle: str
    severity: str
    action_text: str
    target: str
    details: str


# --- In-memory stores ---


DEFAULT_SITE_ID = "flavoria"
DEMO_MODE = os.environ.get("FLOWHARMONY_DEMO_MODE", "1") not in {"0", "false", "False"}

SITES: Dict[str, SiteConfig] = {
    DEFAULT_SITE_ID: SiteConfig(
        site_id=DEFAULT_SITE_ID,
        name="Flavoria",
        lunch_window_start=time(11, 0),
        lunch_window_end=time(13, 30),
        wave_duration_minutes=30,
        portion_grams=160,
        pan_capacity_portions=20,
        dish_name="Salmon"
    )
}

DISHES: Dict[str, Dish] = {
    "salmon": Dish("salmon", "Nordic Salmon", "fish", 160, 5.4, 22.0),
    "lentil": Dish("lentil", "Smoky Lentil Stew", "veg", 180, 1.8, 6.5),
    "chicken": Dish("chicken", "Herb Chicken", "meat", 170, 4.1, 16.0),
    "pasta": Dish("pasta", "Autumn Veggie Pasta", "veg", 150, 1.2, 5.2),
    "cod": Dish("cod", "Citrus Cod", "fish", 155, 4.8, 19.0),
    "tofu": Dish("tofu", "Gochujang Tofu Bowl", "veg", 170, 1.6, 7.8),
    "risotto": Dish("risotto", "Porcini Risotto", "veg", 180, 2.3, 9.4),
    "beef": Dish("beef", "Slow Cooked Beef", "meat", 190, 6.2, 25.5),
    "curry": Dish("curry", "Coconut Pumpkin Curry", "veg", 165, 1.5, 6.9),
    "pea_soup": Dish("pea_soup", "Smoked Pea Soup", "soup", 320, 0.9, 3.8),
    "tomato_soup": Dish("tomato_soup", "Roasted Tomato Soup", "soup", 310, 0.8, 3.4),
    "bbq_pork": Dish("bbq_pork", "BBQ Pulled Pork", "grill", 185, 5.9, 21.5),
    "grill_salmon": Dish("grill_salmon", "Charred Herb Salmon", "grill", 180, 5.7, 24.0),
    "grill_halloumi": Dish("grill_halloumi", "Halloumi & Veg Skewers", "grill", 165, 2.4, 12.5),
    "wd_salmon": Dish("wd_salmon", "Weigh&Dine Citrus Salmon", "weigh", 150, 4.9, 18.5),
    "wd_meatballs": Dish("wd_meatballs", "Weigh&Dine Nordic Meatballs", "weigh", 160, 4.4, 15.8),
    "wd_lentil": Dish("wd_lentil", "Weigh&Dine Lentil Roast", "weigh", 155, 1.9, 7.2),
}

SERVICE_DAYS: Dict[tuple[str, date], ServiceDay] = {}
MENU_DAYS: Dict[str, List[MenuDay]] = {}
MENU_DAYS_REFRESHED_AT: Dict[str, Optional[date]] = {}
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
MENU_DATA_PATH = DATA_DIR / "menu_days.json"
MENU_DATA_LOADED = False

MENU_DATA_VERSION = 3

PRIMARY_SERVICE_SLOT = "favourite_1"

SERVICE_SLOT_CONFIGS: List[Dict[str, object]] = [
    {
        "service_slot": "favourite_1",
        "slot_label": "Favourite 1",
        "station": "Lunch Line",
        "cover_ratio": 0.33,
        "min_covers": 120,
        "swap_focus": True,
        "status_default": "suggested",
        "savings_weight": 1.0,
        "planned_kg": 11.5,
        "sold_pct": 0.45,
        "forecast_pct": 0.4,
        "pan_level_base": 48,
        "satisfaction_base": 78,
        "main_cycle": ["salmon", "beef", "cod", "chicken", "risotto"],
        "recommended_cycle": ["lentil", "tofu", "risotto", "pasta", "curry"],
    },
    {
        "service_slot": "favourite_2",
        "slot_label": "Favourite 2",
        "station": "Lunch Line",
        "cover_ratio": 0.24,
        "min_covers": 90,
        "swap_focus": True,
        "status_default": "suggested",
        "savings_weight": 0.75,
        "planned_kg": 10.0,
        "sold_pct": 0.38,
        "forecast_pct": 0.34,
        "pan_level_base": 44,
        "satisfaction_base": 74,
        "main_cycle": ["chicken", "salmon", "beef", "risotto", "cod"],
        "recommended_cycle": ["pasta", "lentil", "tofu", "curry", "risotto"],
    },
    {
        "service_slot": "vegan",
        "slot_label": "From the Field (Vegan)",
        "station": "Lunch Line",
        "cover_ratio": 0.18,
        "min_covers": 70,
        "swap_focus": True,
        "status_default": "suggested",
        "savings_weight": 0.65,
        "planned_kg": 9.5,
        "sold_pct": 0.31,
        "forecast_pct": 0.28,
        "pan_level_base": 52,
        "satisfaction_base": 82,
        "main_cycle": ["tofu", "lentil", "curry", "pasta", "risotto"],
        "recommended_cycle": ["lentil", "risotto", "tofu", "curry", "pasta"],
    },
    {
        "service_slot": "soup",
        "slot_label": "Soup Bowl",
        "station": "Soup Station",
        "cover_ratio": 0.14,
        "min_covers": 60,
        "swap_focus": False,
        "status_default": "monitor",
        "savings_weight": 0.45,
        "planned_kg": 13.5,
        "sold_pct": 0.26,
        "forecast_pct": 0.24,
        "pan_level_base": 58,
        "satisfaction_base": 80,
        "main_cycle": ["pea_soup", "tomato_soup", "pea_soup", "tomato_soup", "pea_soup"],
        "recommended_cycle": ["tomato_soup", "lentil", "pea_soup", "curry", "pea_soup"],
    },
    {
        "service_slot": "grill",
        "slot_label": "Grill",
        "station": "Grill",
        "cover_ratio": 0.08,
        "min_covers": 45,
        "swap_focus": False,
        "status_default": "monitor",
        "savings_weight": 0.55,
        "planned_kg": 12.0,
        "sold_pct": 0.29,
        "forecast_pct": 0.26,
        "pan_level_base": 62,
        "satisfaction_base": 76,
        "main_cycle": ["bbq_pork", "grill_salmon", "grill_halloumi", "bbq_pork", "grill_salmon"],
        "recommended_cycle": ["grill_halloumi", "grill_salmon", "tofu", "grill_halloumi", "grill_salmon"],
    },
    {
        "service_slot": "weigh_protein",
        "slot_label": "Weigh & Dine – Protein",
        "station": "Weigh & Dine",
        "cover_ratio": 0.07,
        "min_covers": 40,
        "swap_focus": False,
        "status_default": "monitor",
        "savings_weight": 0.5,
        "planned_kg": 14.0,
        "sold_pct": 0.24,
        "forecast_pct": 0.22,
        "pan_level_base": 48,
        "satisfaction_base": 79,
        "main_cycle": ["wd_salmon", "wd_meatballs", "bbq_pork", "wd_salmon", "wd_meatballs"],
        "recommended_cycle": ["wd_lentil", "tofu", "lentil", "wd_lentil", "tofu"],
    },
    {
        "service_slot": "weigh_green",
        "slot_label": "Weigh & Dine – Plant",
        "station": "Weigh & Dine",
        "cover_ratio": 0.06,
        "min_covers": 35,
        "swap_focus": False,
        "status_default": "monitor",
        "savings_weight": 0.45,
        "planned_kg": 12.5,
        "sold_pct": 0.22,
        "forecast_pct": 0.2,
        "pan_level_base": 55,
        "satisfaction_base": 83,
        "main_cycle": ["wd_lentil", "pasta", "curry", "wd_lentil", "risotto"],
        "recommended_cycle": ["pasta", "tofu", "lentil", "risotto", "tofu"],
    },
]

SLOT_CONFIG_MAP = {cfg["service_slot"]: cfg for cfg in SERVICE_SLOT_CONFIGS}

MOCK_MENU_TEMPLATES: List[Dict[str, object]] = [
    {
        "weather": "Rain 4°C · Gusty",
        "predicted_covers": 360,
        "main": "salmon",
        "recommended": "lentil",
        "uptake": 0.58,
        "savings_kg": 2.1,
        "status": "suggested",
    },
    {
        "weather": "Cloudy 6°C",
        "predicted_covers": 342,
        "main": "chicken",
        "recommended": "pasta",
        "uptake": 0.51,
        "savings_kg": 1.6,
        "status": "suggested",
    },
    {
        "weather": "Sleet 2°C · Wind",
        "predicted_covers": 328,
        "main": "beef",
        "recommended": "tofu",
        "uptake": 0.47,
        "savings_kg": 2.4,
        "status": "approved",
    },
    {
        "weather": "Sun 9°C",
        "predicted_covers": 355,
        "main": "cod",
        "recommended": "risotto",
        "uptake": 0.62,
        "savings_kg": 1.2,
        "status": "ignored",
    },
    {
        "weather": "Showers 5°C",
        "predicted_covers": 330,
        "main": "beef",
        "recommended": "curry",
        "uptake": 0.49,
        "savings_kg": 2.8,
        "status": "suggested",
    },
    {
        "weather": "Drizzle 7°C",
        "predicted_covers": 318,
        "main": "salmon",
        "recommended": "tofu",
        "uptake": 0.53,
        "savings_kg": 1.4,
        "status": "approved",
    },
    {
        "weather": "Frost -1°C",
        "predicted_covers": 296,
        "main": "chicken",
        "recommended": "risotto",
        "uptake": 0.44,
        "savings_kg": 1.9,
        "status": "ignored",
    },
]

LIVE_EXTRA_DISHES: List[Dict[str, object]] = [
    {"dish_name": "Chili sin Carne", "planned_kg": 14.0, "sold_pct": 0.42, "forecast_pct": 0.38, "pan_level": 36, "satisfaction": 74},
    {"dish_name": "Tofu Power Bowl", "planned_kg": 11.5, "sold_pct": 0.33, "forecast_pct": 0.35, "pan_level": 54, "satisfaction": 81},
    {"dish_name": "Root Veg Mash", "planned_kg": 9.0, "sold_pct": 0.28, "forecast_pct": 0.25, "pan_level": 62, "satisfaction": 69},
]

SERVICE_DAY_PRESETS: Dict[str, Dict[str, object]] = {
    DEFAULT_SITE_ID: {
        "total_expected_diners": 360,
        "diners_so_far": 148,
        "pan_fill_percent": 46,
        "feedback_scores": [3, 3, 2, 3, 3, 1, 3, 2, 3, 2, 3, 3],
        "predicted_waste_if_no_action_kg": 18.6,
        "predicted_waste_with_flowharmony_kg": 3.4,
        "predicted_waste_savings_eur": 94.0,
        "actual_waste_baseline_kg": 22.0,
        "actual_waste_kg": 6.4,
        "actual_co2_saved_kg": 312.0,
        "actual_money_saved_eur": 548.0,
        "wave_index": 2,
    }
}

DISH_EMOJI = {
    "fish": "🐟",
    "veg": "🥦",
    "meat": "🍖",
    "poultry": "🍗",
    "soup": "🥣",
    "grill": "🔥",
    "weigh": "🍱",
}

CHAIN_SITE_OPTIONS: List[Dict[str, object]] = [
    {"site_id": "flavoria", "name": "Flavoria", "district": "Campus", "avg_wait": 6},
    {"site_id": "campus_west", "name": "Campus West", "district": "Campus", "avg_wait": 11},
    {"site_id": "harbor_breeze", "name": "Harbor Breeze", "district": "City Center", "avg_wait": 8},
    {"site_id": "nordic_green", "name": "Nordic Green", "district": "North Hub", "avg_wait": 5},
]

FEEDBACK_SCREENS: List[Dict[str, object]] = [
    {
        "slug": "smell",
        "question_set": "sensory",
        "response_key": "smell",
        "title": "👃 Tuoksutesti",
        "prompt": "Kuinka houkuttelevalta annos tuoksuu juuri nyt?",
        "options": [
            {"value": "1", "label": "😶 Lattea"},
            {"value": "3", "label": "😊 Mukava"},
            {"value": "5", "label": "🤩 Huikea"},
        ],
        "button_text": "Lähetä tuoksuarvio",
        "success_text": "Kiitos – keittiö hienosäätää aromin!",
    },
    {
        "slug": "appearance",
        "question_set": "sensory",
        "response_key": "appearance",
        "title": "👀 Lautasen ulkoasu",
        "prompt": "Miltä annos näyttää lautasellasi?",
        "options": [
            {"value": "1", "label": "😐 Vaatii huomiota"},
            {"value": "3", "label": "😀 Hyvän näköinen"},
            {"value": "5", "label": "🌟 Täydellinen"},
        ],
        "button_text": "Kerro ulkoasusta",
        "success_text": "Kiitos – esillepano-tiimi ilahtuu!",
    },
    {
        "slug": "temperature",
        "question_set": "sensory",
        "response_key": "temperature",
        "title": "🌡️ Lämpötila",
        "prompt": "Lämpötila tuntuu…",
        "options": [
            {"value": "too_cold", "label": "🥶 Liian kylmä"},
            {"value": "just_right", "label": "😌 Juuri sopiva"},
            {"value": "too_hot", "label": "🔥 Liian kuuma"},
        ],
        "button_text": "Lähetä lämpötunne",
        "success_text": "Kiitos – säädämme lämpöjä!",
    },
    {
        "slug": "wait",
        "question_set": "queue",
        "response_key": "wait_time",
        "title": "⏱️ Jonokokemus",
        "prompt": "Miltä odotus äsken tuntui?",
        "options": [
            {"value": "quick", "label": "🚀 Todella nopea"},
            {"value": "ok", "label": "🙂 Sopiva"},
            {"value": "too_long", "label": "⏳ Liian pitkä"},
        ],
        "button_text": "Kerro jonosta",
        "success_text": "Kiitos – flow-kapteeni hoitaa!",
    },
    {
        "slug": "flow",
        "question_set": "queue",
        "response_key": "flow_speed",
        "title": "🚶‍♀️ Linjan liike",
        "prompt": "Kuvaile linjan etenemistä",
        "options": [
            {"value": "smooth", "label": "🟢 Sujuvaa"},
            {"value": "stop_and_go", "label": "🟡 Pysähtelevää"},
            {"value": "stalled", "label": "🔴 Pysähtynyt"},
        ],
        "button_text": "Lähetä linjan tila",
        "success_text": "Flow-tiimi reagoi heti!",
    },
    {
        "slug": "staff",
        "question_set": "queue",
        "response_key": "staff_warmth",
        "title": "🤝 Palvelun lämpö",
        "prompt": "Kuinka ystävällinen tiimi oli äsken?",
        "options": [
            {"value": "1", "label": "😶"},
            {"value": "3", "label": "🙂"},
            {"value": "5", "label": "🤗"},
        ],
        "button_text": "Lähetä terveinen",
        "success_text": "Kiitos – välitämme kehut!",
    },
    {
        "slug": "portion",
        "question_set": "value",
        "response_key": "portion_size",
        "title": "🍽️ Annoksen koko",
        "prompt": "Annos tuntuu…",
        "options": [
            {"value": "too_light", "label": "🥄 Liian pieni"},
            {"value": "just_right", "label": "🍛 Sopiva"},
            {"value": "too_heavy", "label": "🍱 Liian suuri"},
        ],
        "button_text": "Lähetä annospalaute",
        "success_text": "Kiitos – tasapainotamme annosta!",
    },
    {
        "slug": "value",
        "question_set": "value",
        "response_key": "value_for_money",
        "title": "💶 Vastinetta rahalle",
        "prompt": "Miltä hinta-laatusuhde tuntuu?",
        "options": [
            {"value": "1", "label": "💔 Heikko"},
            {"value": "3", "label": "🙂 Ok"},
            {"value": "5", "label": "🤩 Erinomainen"},
        ],
        "button_text": "Lähetä arvio",
        "success_text": "Kiitos – talouskin kuulee tämän!",
    },
    {
        "slug": "mood",
        "question_set": "value",
        "response_key": "overall_mood",
        "title": "😊 Tunnelmamittari",
        "prompt": "Lähden linjalta fiiliksellä…",
        "options": [
            {"value": "energized", "label": "⚡ Energinen"},
            {"value": "content", "label": "🙂 Tyytyväinen"},
            {"value": "neutral", "label": "😐 Neutraali"},
        ],
        "button_text": "Jaa fiilis",
        "success_text": "Kiitos – pidämme fiiliksen korkealla!",
    },
]

SCREEN_BY_SLUG: Dict[str, Dict[str, object]] = {screen["slug"]: screen for screen in FEEDBACK_SCREENS}
SCREEN_BY_RESPONSE_KEY: Dict[str, Dict[str, object]] = {screen["response_key"]: screen for screen in FEEDBACK_SCREENS}
FEEDBACK_LABELS: Dict[str, str] = {key: screen["title"] for key, screen in SCREEN_BY_RESPONSE_KEY.items()}
QUESTION_SET_TO_ROUTE: Dict[str, str] = {
    "sensory": "feedback1",
    "queue": "feedback2",
    "value": "feedback3",
}
DEFAULT_SCREEN_FOR_ROUTE: Dict[str, str] = {
    "feedback1": next((screen["slug"] for screen in FEEDBACK_SCREENS if screen["question_set"] == "sensory"), FEEDBACK_SCREENS[0]["slug"] if FEEDBACK_SCREENS else ""),
    "feedback2": next((screen["slug"] for screen in FEEDBACK_SCREENS if screen["question_set"] == "queue"), FEEDBACK_SCREENS[0]["slug"] if FEEDBACK_SCREENS else ""),
    "feedback3": next((screen["slug"] for screen in FEEDBACK_SCREENS if screen["question_set"] == "value"), FEEDBACK_SCREENS[0]["slug"] if FEEDBACK_SCREENS else ""),
}

LUNCH_PULSE_REACTIONS: Dict[str, Dict[str, str]] = {
    "1": {"emoji": "😕", "label": "Needs work"},
    "2": {"emoji": "🙂", "label": "Okay"},
    "3": {"emoji": "😋", "label": "Great"},
}

REACTION_LOOKUP: Dict[str, Dict[str, Dict[str, str]]] = {}
for screen in FEEDBACK_SCREENS:
    response_key = str(screen.get("response_key"))
    options_lookup: Dict[str, Dict[str, str]] = {}
    for option in screen.get("options", []):
        raw_label = str(option.get("label", ""))
        parts = raw_label.split(" ", 1)
        emoji = parts[0]
        text = parts[1] if len(parts) > 1 else screen.get("title", "")
        options_lookup[str(option.get("value"))] = {
            "emoji": emoji,
            "label": text,
            "question_set": screen.get("question_set", ""),
            "title": screen.get("title", ""),
        }
    REACTION_LOOKUP[response_key] = options_lookup

MAX_REACTIONS_STORED = 75
REACTION_RESPONSE_LIMIT = 10
FEEDBACK_SCORE_HISTORY_LIMIT = 100
DETAILED_FEEDBACK_HISTORY_LIMIT = 400
REDIS_ENTRY_TTL_SECONDS = 7 * 24 * 60 * 60


def _redis_available() -> bool:
    return bool(REDIS_CLIENT)


def _service_day_storage_id(service_day: ServiceDay) -> str:
    return f"{service_day.site_id}:{service_day.date.isoformat()}"


def _redis_key(prefix: str, service_day: ServiceDay) -> str:
    return f"fh:{prefix}:{_service_day_storage_id(service_day)}"


def _persist_list_entry(service_day: ServiceDay, prefix: str, payload: object, limit: int) -> None:
    client = REDIS_CLIENT
    if not client:
        return
    key = _redis_key(prefix, service_day)
    try:
        data = json.dumps(payload)
        pipe = client.pipeline()
        pipe.rpush(key, data)
        pipe.ltrim(key, -limit, -1)
        pipe.expire(key, REDIS_ENTRY_TTL_SECONDS)
        pipe.execute()
    except (RedisError, TypeError) as exc:  # pragma: no cover - non critical persistence
        logger.debug("Failed to persist %s entry: %s", prefix, exc)


def _load_list_entries(service_day: ServiceDay, prefix: str, limit: int) -> List[object]:
    client = REDIS_CLIENT
    if not client:
        return []
    key = _redis_key(prefix, service_day)
    try:
        # Fetch the newest entries up to the provided limit.
        raw_entries = client.lrange(key, -limit, -1)
    except RedisError as exc:  # pragma: no cover - fallback to in-memory data
        logger.debug("Failed to fetch %s entries: %s", prefix, exc)
        return []
    parsed: List[object] = []
    for raw in raw_entries or []:
        try:
            parsed.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return parsed


def _persist_feedback_score(service_day: ServiceDay, score: int) -> None:
    _persist_list_entry(service_day, "scores", score, FEEDBACK_SCORE_HISTORY_LIMIT)


def _persist_detailed_feedback(service_day: ServiceDay, entry: Dict[str, object]) -> None:
    _persist_list_entry(service_day, "detailed_feedback", entry, DETAILED_FEEDBACK_HISTORY_LIMIT)


def _persist_reaction(service_day: ServiceDay, reaction: Dict[str, object]) -> None:
    _persist_list_entry(service_day, "reactions", reaction, MAX_REACTIONS_STORED)


def get_feedback_scores(service_day: ServiceDay) -> List[int]:
    if _redis_available():
        cached = _load_list_entries(service_day, "scores", FEEDBACK_SCORE_HISTORY_LIMIT)
        if cached:
            parsed_scores: List[int] = []
            for entry in cached:
                try:
                    parsed_scores.append(int(entry))
                except (TypeError, ValueError):
                    continue
            if parsed_scores:
                service_day.feedback_scores = parsed_scores
                return parsed_scores
    return list(service_day.feedback_scores[-FEEDBACK_SCORE_HISTORY_LIMIT:])


def get_detailed_feedback_entries(service_day: ServiceDay, limit: int) -> List[Dict[str, object]]:
    if _redis_available():
        cached = _load_list_entries(service_day, "detailed_feedback", max(limit, DETAILED_FEEDBACK_HISTORY_LIMIT))
        if cached:
            entries: List[Dict[str, object]] = [entry for entry in cached if isinstance(entry, dict)]
            if entries:
                service_day.detailed_feedback = entries
                return entries[-limit:]
    return list(service_day.detailed_feedback[-limit:])


def _hydrate_service_day_from_store(service_day: ServiceDay) -> None:
    if not _redis_available():
        return
    reactions = _load_list_entries(service_day, "reactions", MAX_REACTIONS_STORED)
    if reactions:
        service_day.reaction_stream = [entry for entry in reactions if isinstance(entry, dict)]
    scores_raw = _load_list_entries(service_day, "scores", FEEDBACK_SCORE_HISTORY_LIMIT)
    if scores_raw:
        parsed_scores: List[int] = []
        for raw in scores_raw:
            try:
                parsed_scores.append(int(raw))
            except (TypeError, ValueError):
                continue
        if parsed_scores:
            service_day.feedback_scores = parsed_scores
    detailed_entries = _load_list_entries(service_day, "detailed_feedback", DETAILED_FEEDBACK_HISTORY_LIMIT)
    if detailed_entries:
        service_day.detailed_feedback = [entry for entry in detailed_entries if isinstance(entry, dict)]


def _clear_service_day_storage(service_day: ServiceDay) -> None:
    client = REDIS_CLIENT
    if not client:
        return
    keys = [
        _redis_key("reactions", service_day),
        _redis_key("scores", service_day),
        _redis_key("detailed_feedback", service_day),
    ]
    try:
        client.delete(*keys)
    except RedisError as exc:  # pragma: no cover
        logger.debug("Failed to clear Redis keys %s: %s", keys, exc)


def append_reaction_entry(
    service_day: ServiceDay,
    *,
    response_key: str,
    value: Any,
    question_set: Optional[str] = None,
    title: Optional[str] = None,
    label: Optional[str] = None,
    emoji: Optional[str] = None,
    source: Optional[str] = None,
) -> None:
    lookup = REACTION_LOOKUP.get(str(response_key), {})
    entry_meta = lookup.get(str(value)) if lookup else None
    reaction: Dict[str, object] = {
        "id": uuid4().hex,
        "timestamp": datetime.utcnow().isoformat(),
        "response_key": str(response_key),
        "value": str(value),
        "emoji": (entry_meta or {}).get("emoji") or emoji or "✨",
        "label": (entry_meta or {}).get("label") or label or str(value),
        "question_set": (entry_meta or {}).get("question_set") or question_set or "",
        "title": (entry_meta or {}).get("title") or title or "",
        "source": source or (question_set or "general"),
    }
    question_set_key = reaction.get("question_set")
    route = QUESTION_SET_TO_ROUTE.get(str(question_set_key)) if question_set_key else None
    if route:
        reaction["route"] = route
    service_day.reaction_stream.append(reaction)
    if len(service_day.reaction_stream) > MAX_REACTIONS_STORED:
        service_day.reaction_stream[:] = service_day.reaction_stream[-MAX_REACTIONS_STORED:]
    _persist_reaction(service_day, reaction)


def get_recent_reactions(service_day: ServiceDay, limit: int = REACTION_RESPONSE_LIMIT) -> List[Dict[str, object]]:
    if limit <= 0:
        return []
    if _redis_available():
        entries = _load_list_entries(service_day, "reactions", max(limit, MAX_REACTIONS_STORED))
        if entries:
            typed_entries: List[Dict[str, object]] = [dict(entry) for entry in entries if isinstance(entry, dict)]
            if typed_entries:
                service_day.reaction_stream = typed_entries
                return typed_entries[-limit:]
    if not service_day.reaction_stream:
        return []
    recent = service_day.reaction_stream[-limit:]
    return [dict(entry) for entry in recent]

DETAILED_FEEDBACK_WINDOW_MINUTES = 90
MAX_DETAILED_FEEDBACK_SAMPLES = 160
NUMERIC_FEEDBACK_WEIGHTS: Dict[str, float] = {
    "smell": 0.05,
    "appearance": 0.04,
    "staff_warmth": 0.03,
    "value_for_money": 0.06,
}
CATEGORICAL_FEEDBACK_WEIGHTS: Dict[str, Dict[str, float]] = {
    "temperature": {"too_cold": -0.05, "just_right": 0.02, "too_hot": -0.03},
    "wait_time": {"quick": 0.05, "ok": 0.0, "too_long": -0.08},
    "flow_speed": {"smooth": 0.04, "stop_and_go": -0.03, "stalled": -0.07},
    "portion_size": {"too_light": -0.05, "just_right": 0.03, "too_heavy": -0.02},
    "overall_mood": {"energized": 0.05, "content": 0.02, "neutral": -0.015},
}


def get_wave_template(site: SiteConfig) -> List[WaveState]:
    waves: List[WaveState] = []
    current = datetime.combine(date.today(), site.lunch_window_start)
    end = datetime.combine(date.today(), site.lunch_window_end)
    expected_pattern = [10, 30, 40, 25, 15]
    idx = 0
    while current.time() < end.time() and idx < len(expected_pattern):
        next_slot = current + timedelta(minutes=site.wave_duration_minutes)
        waves.append(
            WaveState(
                index=idx,
                start_time=current.time(),
                end_time=next_slot.time(),
                expected_diners_pattern=expected_pattern[idx]
            )
        )
        current = next_slot
        idx += 1
    return waves


def generate_menu_plan(site_id: str, start_date: date, days: int) -> List[MenuDay]:
    rows: List[MenuDay] = []
    templates = MOCK_MENU_TEMPLATES or [{}]
    template_count = len(templates)
    for offset in range(days):
        day = start_date + timedelta(days=offset)
        tpl = templates[offset % template_count]
        weather = str(tpl.get("weather", "Cloudy"))
        cover_baseline = int(tpl.get("predicted_covers", 320))
        trend_adjustment = offset * 6
        total_predicted = max(240, cover_baseline - trend_adjustment)
        uptake_base = float(tpl.get("uptake", 0.5))
        savings_base = float(tpl.get("savings_kg", 1.2))
        status_base = str(tpl.get("status", "suggested"))

        day_entries: List[MenuDay] = []
        for slot_index, slot_cfg in enumerate(SERVICE_SLOT_CONFIGS):
            main_cycle = slot_cfg.get("main_cycle", []) or ["salmon"]
            rec_cycle = slot_cfg.get("recommended_cycle", []) or ["lentil"]
            main_id = main_cycle[(offset + slot_index) % len(main_cycle)]
            rec_id = rec_cycle[(offset + slot_index) % len(rec_cycle)]
            main = DISHES.get(main_id, DISHES["salmon"])
            recommended = DISHES.get(rec_id, DISHES["lentil"])
            ratio = float(slot_cfg.get("cover_ratio", 0.2))
            min_covers = int(slot_cfg.get("min_covers", 40))
            slot_covers = max(min_covers, int(total_predicted * ratio))
            slot_uptake = uptake_base - 0.012 * offset + 0.015 * slot_index
            slot_uptake = max(0.25, min(slot_uptake, 0.88))
            savings_weight = float(slot_cfg.get("savings_weight", 0.6))
            slot_savings = round(max(0.2, savings_base * savings_weight - 0.04 * offset), 1)
            swap_focus = bool(slot_cfg.get("swap_focus", False))
            status = slot_cfg.get("status_default", "monitor")
            if swap_focus:
                status = "suggested" if offset < 2 else status_base

            day_entries.append(
                MenuDay(
                    date=day,
                    weather=weather,
                    predicted_covers=slot_covers,
                    main_dish_planned=main,
                    recommended_dish=recommended,
                    service_slot=str(slot_cfg["service_slot"]),
                    slot_label=str(slot_cfg.get("slot_label", slot_cfg["service_slot"])),
                    station=str(slot_cfg.get("station")) if slot_cfg.get("station") else None,
                    uptake_if_kept=slot_uptake,
                    suggested_swap_savings_kg=slot_savings,
                    suggested_swap_savings_eur=round(slot_savings * main.cost_per_kg, 1),
                    swap_status=status,
                )
            )

        primary_entry = next((entry for entry in day_entries if entry.service_slot == PRIMARY_SERVICE_SLOT), None)
        if primary_entry:
            primary_entry.additional_dishes = [entry.main_dish_planned for entry in day_entries if entry is not primary_entry]
        rows.extend(day_entries)
    return rows


def menu_day_to_dict(menu: MenuDay) -> Dict[str, object]:
    return {
        "date": menu.date.isoformat(),
        "weather": menu.weather,
        "predicted_covers": menu.predicted_covers,
        "main_dish_id": menu.main_dish_planned.dish_id,
        "recommended_dish_id": menu.recommended_dish.dish_id,
        "service_slot": menu.service_slot,
        "slot_label": menu.slot_label,
        "station": menu.station,
        "uptake_if_kept": menu.uptake_if_kept,
        "suggested_swap_savings_kg": menu.suggested_swap_savings_kg,
        "suggested_swap_savings_eur": menu.suggested_swap_savings_eur,
        "swap_status": menu.swap_status,
        "additional_dish_ids": [dish.dish_id for dish in menu.additional_dishes],
    }


def menu_day_from_dict(payload: Dict[str, object]) -> MenuDay:
    main_id = str(payload.get("main_dish_id", "")).lower() or "salmon"
    rec_id = str(payload.get("recommended_dish_id", "")).lower() or "lentil"
    main_dish = DISHES.get(main_id, DISHES["salmon"])
    recommended_dish = DISHES.get(rec_id, DISHES["lentil"])
    day = datetime.fromisoformat(payload["date"]).date()
    additional_ids = payload.get("additional_dish_ids", [])
    additional_dishes = [DISHES.get(str(did).lower(), DISHES["lentil"]) for did in additional_ids]
    service_slot = str(payload.get("service_slot", PRIMARY_SERVICE_SLOT))
    cfg = SLOT_CONFIG_MAP.get(service_slot)
    slot_label = payload.get("slot_label") or (cfg["slot_label"] if cfg else "Favourite 1")
    station = payload.get("station") or (cfg.get("station") if cfg else None)
    return MenuDay(
        date=day,
        weather=payload.get("weather"),
        predicted_covers=int(payload.get("predicted_covers", 0)),
        main_dish_planned=main_dish,
        recommended_dish=recommended_dish,
        service_slot=service_slot,
        slot_label=str(slot_label),
        station=station,
        uptake_if_kept=float(payload.get("uptake_if_kept", 0.0)),
        suggested_swap_savings_kg=float(payload.get("suggested_swap_savings_kg", 0.0)),
        suggested_swap_savings_eur=float(payload.get("suggested_swap_savings_eur", 0.0)),
        swap_status=str(payload.get("swap_status", "suggested")),
        additional_dishes=additional_dishes,
    )


def ensure_menu_storage_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def persist_menu_days() -> None:
    ensure_menu_storage_dir()
    data = {
        "version": MENU_DATA_VERSION,
        "sites": {
            site_id: [menu_day_to_dict(row) for row in rows]
            for site_id, rows in MENU_DAYS.items()
        },
        "refreshed_at": {
            site_id: MENU_DAYS_REFRESHED_AT.get(site_id).isoformat() if MENU_DAYS_REFRESHED_AT.get(site_id) else None
            for site_id in SITES
        },
    }
    with MENU_DATA_PATH.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)


def load_menu_data() -> None:
    global MENU_DATA_LOADED
    if MENU_DATA_LOADED:
        return
    ensure_menu_storage_dir()
    ensure_save = False
    today = date.today()
    if MENU_DATA_PATH.exists():
        try:
            with MENU_DATA_PATH.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except json.JSONDecodeError:
            payload = {}
        version = payload.get("version")
        if version == MENU_DATA_VERSION:
            sites_payload = payload.get("sites", {})
            refreshed_map = payload.get("refreshed_at", {})
            for site_id in SITES:
                rows_payload = sites_payload.get(site_id, [])
                if rows_payload:
                    MENU_DAYS[site_id] = [menu_day_from_dict(row) for row in rows_payload]
                else:
                    MENU_DAYS[site_id] = generate_menu_plan(site_id, today, 7)
                    ensure_save = True
                refreshed_value = refreshed_map.get(site_id)
                if refreshed_value:
                    try:
                        MENU_DAYS_REFRESHED_AT[site_id] = datetime.fromisoformat(refreshed_value).date()
                    except ValueError:
                        MENU_DAYS_REFRESHED_AT[site_id] = None
                else:
                    MENU_DAYS_REFRESHED_AT[site_id] = None
        else:
            ensure_save = True
    else:
        ensure_save = True
    if ensure_save:
        for site_id in SITES:
            MENU_DAYS[site_id] = generate_menu_plan(site_id, today, 7)
            MENU_DAYS_REFRESHED_AT[site_id] = today
        persist_menu_days()
    MENU_DATA_LOADED = True


def get_current_menu_day(site_id: str, target_date: Optional[date] = None, service_slot: Optional[str] = None) -> Optional[MenuDay]:
    ensure_menu_days_current(site_id)
    target = target_date or date.today()
    rows = MENU_DAYS.get(site_id, [])
    if service_slot:
        for menu in rows:
            if menu.date == target and menu.service_slot == service_slot:
                return menu
    for menu in rows:
        if menu.date == target:
            return menu
    return rows[0] if rows else None


def get_menu_days_for_date(site_id: str, target_date: date) -> List[MenuDay]:
    ensure_menu_days_current(site_id)
    rows = MENU_DAYS.get(site_id, [])
    return [menu for menu in rows if menu.date == target_date]


def ensure_menu_days_current(site_id: str) -> None:
    load_menu_data()
    today = date.today()
    if MENU_DAYS_REFRESHED_AT.get(site_id) == today:
        return
    rows = [row for row in MENU_DAYS.get(site_id, []) if row.date >= today]
    rows.sort(key=lambda m: (m.date, m.service_slot))
    existing_dates = sorted({row.date for row in rows})
    needed = 7 - len(existing_dates)
    if needed > 0:
        start_date = today if not existing_dates else existing_dates[-1] + timedelta(days=1)
        rows.extend(generate_menu_plan(site_id, start_date, needed))
    rows.sort(key=lambda m: (m.date, m.service_slot))
    MENU_DAYS[site_id] = rows
    MENU_DAYS_REFRESHED_AT[site_id] = today
    persist_menu_days()


# --- Forecast adapter ---


class ForecastAdapter:
    def predict_next_wave(self, site: SiteConfig, service_day: ServiceDay, wave_index: int, feedback_multiplier: float = 1.0) -> int:
        raise NotImplementedError


class StubForecastAdapter(ForecastAdapter):
    EXPECTED_WAVES = [10, 30, 40, 25, 15]

    def predict_next_wave(self, site: SiteConfig, service_day: ServiceDay, wave_index: int, feedback_multiplier: float = 1.0) -> int:
        idx = min(max(wave_index, 0), len(self.EXPECTED_WAVES) - 1)
        expected_in_wave = self.EXPECTED_WAVES[idx]
        weekday = datetime.now().weekday()
        if weekday >= 5:
            expected_in_wave = int(expected_in_wave * 0.7)

        remaining_expected = max(service_day.total_expected_diners - service_day.diners_so_far, 0)
        total_waves = len(self.EXPECTED_WAVES)
        remaining_waves = max(total_waves - wave_index, 1)
        base_for_wave = remaining_expected / remaining_waves
        predicted = 0.5 * expected_in_wave + 0.5 * base_for_wave

        predicted *= feedback_multiplier

        return max(int(predicted), 0)


forecast_adapter = StubForecastAdapter()


# --- Core helpers ---


def get_site_config(site_id: str) -> SiteConfig:
    return SITES.get(site_id, SITES[DEFAULT_SITE_ID])


def resolve_display_dish(site: SiteConfig, site_id: str, service_day: ServiceDay) -> tuple[Dish, Optional[MenuDay]]:
    menu_day = get_current_menu_day(site_id, service_day.date, PRIMARY_SERVICE_SLOT)
    if menu_day:
        if menu_day.swap_status == "approved":
            return menu_day.recommended_dish, menu_day
        return menu_day.main_dish_planned, menu_day
    key = site.dish_name.lower()
    dish = DISHES.get(key)
    if dish:
        return dish, None
    # fallback dish synthesized from site configuration
    fallback = Dish(
        dish_id=key or "kitchen",
        name=site.dish_name,
        type="veg",
        avg_portion_grams=site.portion_grams,
        co2_per_kg=2.0,
        cost_per_kg=8.0,
    )
    return fallback, None


def get_current_wave_index(now: time, site: SiteConfig) -> int:
    waves = get_wave_template(site)
    for wave in waves:
        if wave.start_time <= now < wave.end_time:
            return wave.index
    if now < waves[0].start_time:
        return waves[0].index
    return waves[-1].index


def get_or_create_service_day(site_id: str, today: Optional[date] = None) -> ServiceDay:
    if today is None:
        today = date.today()
    key = (site_id, today)
    if key not in SERVICE_DAYS:
        site = get_site_config(site_id)
        waves = get_wave_template(site)
        preset = SERVICE_DAY_PRESETS.get(site_id, {})
        SERVICE_DAYS[key] = ServiceDay(
            site_id=site_id,
            date=today,
            total_expected_diners=int(preset.get("total_expected_diners", 120)),
            diners_so_far=int(preset.get("diners_so_far", 0)),
            wave_index=int(preset.get("wave_index", 0)),
            pan_fill_percent=int(preset.get("pan_fill_percent", 60)),
            feedback_scores=list(preset.get("feedback_scores", [])),
            waves=waves,
            predicted_waste_if_no_action_kg=float(preset.get("predicted_waste_if_no_action_kg", 12.0)),
            predicted_waste_with_flowharmony_kg=float(preset.get("predicted_waste_with_flowharmony_kg", 2.1)),
            predicted_waste_savings_eur=float(preset.get("predicted_waste_savings_eur", 55.0)),
            actual_waste_baseline_kg=float(preset.get("actual_waste_baseline_kg", 15.0)),
            actual_waste_kg=(float(preset.get("actual_waste_kg")) if preset.get("actual_waste_kg") is not None else None),
            actual_co2_saved_kg=(float(preset.get("actual_co2_saved_kg")) if preset.get("actual_co2_saved_kg") is not None else None),
            actual_money_saved_eur=(float(preset.get("actual_money_saved_eur")) if preset.get("actual_money_saved_eur") is not None else None),
        )
        _hydrate_service_day_from_store(SERVICE_DAYS[key])
    return SERVICE_DAYS[key]


def compute_suggested_grams(predicted_diners_next_wave: int, pan_fill_percent: int, portion_grams: int, pan_capacity_portions: int) -> int:
    current_portions = (pan_fill_percent / 100.0) * pan_capacity_portions
    portions_needed = max(predicted_diners_next_wave - current_portions, 0)
    suggested_grams = int(portions_needed * portion_grams)
    return max(0, min(suggested_grams, 4000))


def compute_satisfaction_percent(feedback_scores: List[int]) -> Optional[int]:
    if not feedback_scores:
        return None
    avg = sum(feedback_scores) / len(feedback_scores)
    return int(33 + (avg - 1) * (67 / 2.0))


def compute_feedback_demand_multiplier(
    service_day: ServiceDay,
    feedback_scores: Optional[List[int]] = None,
    detailed_entries: Optional[List[Dict[str, object]]] = None,
) -> tuple[float, Dict[str, float]]:
    adjustments: Dict[str, float] = {}
    total_delta = 0.0

    scores = feedback_scores if feedback_scores is not None else get_feedback_scores(service_day)
    if scores:
        avg_rating = sum(scores) / len(scores)
        rating_delta = (avg_rating - 2.0) * 0.08
        adjustments["tray_rating"] = round(rating_delta, 4)
        total_delta += rating_delta

    entries = detailed_entries if detailed_entries is not None else get_detailed_feedback_entries(service_day, DETAILED_FEEDBACK_HISTORY_LIMIT)
    if entries:
        now = datetime.utcnow()
        cutoff = now - timedelta(minutes=DETAILED_FEEDBACK_WINDOW_MINUTES)
        stats: Dict[str, List[str]] = defaultdict(list)
        samples = 0
        for entry in reversed(entries):
            if samples >= MAX_DETAILED_FEEDBACK_SAMPLES:
                break
            timestamp = entry.get("timestamp")
            entry_time = None
            if timestamp:
                try:
                    entry_time = datetime.fromisoformat(timestamp)
                except ValueError:
                    entry_time = None
            if entry_time and entry_time < cutoff:
                continue
            responses = entry.get("responses") or {}
            for key, value in responses.items():
                stats[key].append(str(value))
            samples += 1

        for key, values in stats.items():
            if key in NUMERIC_FEEDBACK_WEIGHTS:
                numbers: List[float] = []
                for raw in values:
                    try:
                        numbers.append(float(raw))
                    except (TypeError, ValueError):
                        continue
                if not numbers:
                    continue
                avg_value = sum(numbers) / len(numbers)
                normalized = (avg_value - 3.0) / 2.0
                delta = normalized * NUMERIC_FEEDBACK_WEIGHTS[key]
                adjustments[key] = round(delta, 4)
                total_delta += delta
            elif key in CATEGORICAL_FEEDBACK_WEIGHTS:
                mapping = CATEGORICAL_FEEDBACK_WEIGHTS[key]
                weights = [mapping.get(value, 0.0) for value in values]
                if not weights:
                    continue
                delta = sum(weights) / len(weights)
                adjustments[key] = round(delta, 4)
                total_delta += delta

    bounded_delta = max(-0.2, min(total_delta, 0.18))
    multiplier = 1.0 + bounded_delta
    adjustments["combined_delta"] = round(bounded_delta, 4)
    return multiplier, adjustments


def decision_card_as_dict(card: DecisionCard) -> Dict[str, object]:
    payload = asdict(card)
    payload["timestamp"] = card.timestamp.isoformat()
    return payload


def compute_current_state(site_id: str) -> Dict[str, object]:
    site = get_site_config(site_id)
    service_day = get_or_create_service_day(site_id)
    dish, menu_day = resolve_display_dish(site, site_id, service_day)
    now = datetime.now()
    waves = service_day.waves or get_wave_template(site)
    service_day.waves = waves
    current_wave_idx = get_current_wave_index(now.time(), site)
    before_first_wave = bool(waves) and now.time() < waves[0].start_time
    if not waves:
        target_wave_idx = current_wave_idx
    elif before_first_wave:
        target_wave_idx = waves[0].index
    else:
        last_index = waves[-1].index
        target_wave_idx = min(current_wave_idx + 1, last_index)
    service_day.wave_index = target_wave_idx

    feedback_scores = get_feedback_scores(service_day)
    detailed_entries = get_detailed_feedback_entries(service_day, DETAILED_FEEDBACK_HISTORY_LIMIT)
    feedback_multiplier, feedback_adjustments = compute_feedback_demand_multiplier(
        service_day,
        feedback_scores=feedback_scores,
        detailed_entries=detailed_entries,
    )
    lunch_finished = now.time() >= site.lunch_window_end
    predicted = 0
    if not lunch_finished or DEMO_MODE:
        predicted = forecast_adapter.predict_next_wave(site, service_day, target_wave_idx, feedback_multiplier)
    diners_remaining = max(service_day.total_expected_diners - service_day.diners_so_far, 0)
    if DEMO_MODE:
        if lunch_finished and diners_remaining:
            predicted = max(predicted, min(max(diners_remaining // 3, 14), 40))
        if predicted <= 0 and diners_remaining:
            predicted = max(12, min(diners_remaining, 32))
        if service_day.pan_fill_percent >= 75:
            service_day.pan_fill_percent = max(45, service_day.pan_fill_percent - 20)
    suggested_grams = compute_suggested_grams(
        predicted,
        service_day.pan_fill_percent,
        site.portion_grams,
        site.pan_capacity_portions,
    )
    satisfaction = compute_satisfaction_percent(feedback_scores)
    suggested_portions = suggested_grams / site.portion_grams if site.portion_grams else 0
    total_waves = len(waves) if waves else 0
    served_pct = (service_day.diners_so_far / service_day.total_expected_diners * 100) if service_day.total_expected_diners else 0
    note = "Pan already has enough portions for the upcoming wave"
    if suggested_grams:
        note = f"Prep ~{suggested_portions:.1f} portions ({suggested_grams} g) for {predicted} diners next wave"
        if diners_remaining:
            note += f" • {diners_remaining} diners remain today"
    dish_emoji = DISH_EMOJI.get(dish.type, "🍽️")

    if service_day.last_decision and service_day.last_decision.wave_index == target_wave_idx:
        status = service_day.last_decision.status
    else:
        status = "pending"

    service_slot = menu_day.service_slot if menu_day else PRIMARY_SERVICE_SLOT
    slot_cfg = SLOT_CONFIG_MAP.get(service_slot, {})
    slot_label = menu_day.slot_label if menu_day else str(slot_cfg.get("slot_label", "Favourite 1"))
    station_label = menu_day.station if menu_day else slot_cfg.get("station")

    card = DecisionCard(
        site_id=site.site_id,
        wave_index=target_wave_idx,
        timestamp=now,
        predicted_diners_next_wave=predicted,
        suggested_grams=suggested_grams,
        pan_fill_percent=service_day.pan_fill_percent,
        satisfaction_percent=satisfaction,
        dish_name=dish.name,
        dish_type=dish.type,
        suggested_portions=suggested_portions,
        note=note,
        service_slot=service_slot,
        slot_label=slot_label,
        station_label=str(station_label) if station_label else None,
        status=status,
    )
    service_day.last_decision = card

    if suggested_grams >= 1800 or service_day.pan_fill_percent <= 35:
        signal_level = "critical"
        signal_label = "Refill now"
        action_headline = "REFILL PAN NOW"
        action_detail = f"Add {suggested_grams} g (~{suggested_portions:.0f} portions) immediately"
    elif suggested_grams >= 600 or service_day.pan_fill_percent <= 55:
        signal_level = "warning"
        signal_label = "Top up soon"
        action_headline = "TOP UP WITH A SMALL BATCH"
        action_detail = f"Add {suggested_grams} g (~{suggested_portions:.1f} portions) to stay ahead"
    else:
        signal_level = "ready"
        signal_label = "Hold steady"
        action_headline = "PAN IS GOOD FOR NOW"
        action_detail = "Keep an eye on the line – no prep needed this wave"

    supporting_wave_line = slot_label
    if total_waves:
        supporting_wave_line += f" · Wave {target_wave_idx + 1}/{total_waves}"
    if predicted:
        supporting_wave_line += f" · {predicted} diners next"

    if diners_remaining > 0:
        supporting_remaining_line = f"{diners_remaining} diners still expected today"
    else:
        supporting_remaining_line = "Service almost wrapped"

    pan_snapshot = f"Pan ~{service_day.pan_fill_percent}% full"
    if suggested_grams:
        portions_snapshot = f"Prep ~{suggested_portions:.1f} portions ({suggested_grams} g)"
    else:
        portions_snapshot = "No prep batch needed"

    state = {
        "site_id": site.site_id,
        "date": service_day.date.isoformat(),
        "diners_so_far": service_day.diners_so_far,
        "total_expected_today": service_day.total_expected_diners,
        "wave_index": target_wave_idx,
        "total_waves": total_waves,
        "predicted_diners_next_wave": predicted,
        "suggested_grams": suggested_grams,
        "pan_fill_percent": service_day.pan_fill_percent,
        "satisfaction_percent": satisfaction,
        "feedback_multiplier": round(feedback_multiplier, 3),
        "feedback_adjustments": feedback_adjustments,
        "served_percent": round(served_pct, 1),
        "remaining_diners": diners_remaining,
        "current_dish_name": dish.name,
        "current_dish_type": dish.type,
        "dish_emoji": dish_emoji,
        "decision_note": note,
        "suggested_portions": round(suggested_portions, 1),
        "wave_label": f"{target_wave_idx + 1}/{total_waves}" if total_waves else str(target_wave_idx + 1),
        "menu_swap_status": (menu_day.swap_status if menu_day else None),
        "service_slot": service_slot,
        "service_slot_label": slot_label,
        "service_station": station_label,
        "current_slot_wave": f"{slot_label} · Wave {target_wave_idx + 1}/{total_waves}" if total_waves else f"{slot_label} · Wave {target_wave_idx + 1}",
        "last_decision": decision_card_as_dict(card),
        "signal_level": signal_level,
        "signal_label": signal_label,
        "action_headline": action_headline,
        "action_detail": action_detail,
        "supporting_wave_line": supporting_wave_line,
        "supporting_remaining_line": supporting_remaining_line,
        "pan_snapshot": pan_snapshot,
        "portions_snapshot": portions_snapshot,
    }
    state["station_brief"] = build_station_brief(site_id, service_day, feedback_multiplier)
    state["reaction_stream"] = get_recent_reactions(service_day)
    return state


def get_site_id_from_request() -> str:
    return request.args.get("site_id", DEFAULT_SITE_ID)


def compute_today_banner(service_day: ServiceDay) -> TodayBanner:
    date_str = service_day.date.strftime("%A %d %b")
    predicted_covers = service_day.total_expected_diners
    predicted_covers_delta_pct = -5.0
    predicted_waste_no_action_kg = service_day.predicted_waste_if_no_action_kg
    predicted_waste_with_fh_kg = service_day.predicted_waste_with_flowharmony_kg
    waste_cost_per_kg = 9.8
    return TodayBanner(
        date_str=date_str,
        predicted_covers=predicted_covers,
        predicted_covers_delta_pct=predicted_covers_delta_pct,
        predicted_waste_no_action_kg=predicted_waste_no_action_kg,
        predicted_waste_no_action_eur=round(predicted_waste_no_action_kg * waste_cost_per_kg, 1),
        predicted_waste_with_fh_kg=predicted_waste_with_fh_kg,
        predicted_waste_with_fh_eur=round(predicted_waste_with_fh_kg * waste_cost_per_kg, 1),
    )


def build_menu_rows(site_id: str) -> List[MenuPreventionRow]:
    ensure_menu_days_current(site_id)
    rows: List[MenuPreventionRow] = []
    by_date: Dict[date, List[MenuDay]] = {}
    for menu in MENU_DAYS.get(site_id, []):
        by_date.setdefault(menu.date, []).append(menu)
    for target_date in sorted(by_date.keys()):
        daily_entries = sorted(by_date[target_date], key=lambda m: m.service_slot)
        for menu in daily_entries:
            peers = [peer for peer in daily_entries if peer.service_slot != menu.service_slot]
            other_items = ", ".join(peer.main_dish_planned.name for peer in peers[:4])
            slot_cfg = SLOT_CONFIG_MAP.get(menu.service_slot, {})
            swap_focus = bool(slot_cfg.get("swap_focus", False))
            swap_text = (
                f"→ Swap = –{menu.suggested_swap_savings_kg:.1f} kg {menu.main_dish_planned.name.lower()}"
                if swap_focus
                else f"Monitor station (saves {menu.suggested_swap_savings_kg:.1f} kg)"
            )
        rows.append(
            MenuPreventionRow(
                    date_str=menu.date.strftime("%a %d %b"),
                    date_iso=menu.date.isoformat(),
                    weather=menu.weather or "–",
                    predicted_covers=menu.predicted_covers,
                    recommended_dish_name=menu.recommended_dish.name,
                    current_dish_name=menu.main_dish_planned.name,
                    service_slot=menu.service_slot,
                    slot_label=menu.slot_label,
                    station_label=menu.station or "–",
                    uptake_if_kept_pct=int(menu.uptake_if_kept * 100),
                    suggested_swap_text=swap_text,
                    swap_status=menu.swap_status,
                    other_items=other_items,
            )
        )
    return rows


def build_alerts(site_id: str) -> List[AlertItem]:
    ensure_menu_days_current(site_id)
    alerts: List[AlertItem] = []
    today = date.today()
    for menu in MENU_DAYS.get(site_id, []):
        slot_cfg = SLOT_CONFIG_MAP.get(menu.service_slot)
        if not slot_cfg or not slot_cfg.get("swap_focus"):
            continue
        delta = (menu.date - today).days
        if 1 <= delta <= 2 and menu.swap_status == "suggested":
            alerts.append(
                AlertItem(
                    date_str=menu.date.strftime("%A %d %b"),
                    date_iso=menu.date.isoformat(),
                    message=(
                        f"{menu.main_dish_planned.name}: only {int(menu.uptake_if_kept * 100)}% uptake expected "
                        f"({menu.predicted_covers} covers, {menu.weather})"
                    ),
                    action_text=(
                        f"Swap to {menu.recommended_dish.name} now – prevent {menu.suggested_swap_savings_kg:.1f} kg waste "
                        f"+ €{menu.suggested_swap_savings_eur:.0f}"
                    ),
                    from_dish_name=menu.main_dish_planned.name,
                    to_dish_name=menu.recommended_dish.name,
                    service_slot=menu.service_slot,
                    slot_label=menu.slot_label,
                )
            )
    return alerts


def build_live_rows(site_id: str, state_snapshot: Dict[str, object]) -> List[LiveDishRow]:
    site = get_site_config(site_id)
    service_day = get_or_create_service_day(site_id)
    predicted = state_snapshot["predicted_diners_next_wave"]
    portion_kg = site.portion_grams / 1000.0
    sold_kg = service_day.diners_so_far * portion_kg
    primary_dish_name = state_snapshot.get("current_dish_name", site.dish_name)
    menu_day = get_current_menu_day(site_id, service_day.date, PRIMARY_SERVICE_SLOT)
    live_rows = [
        LiveDishRow(
            dish_name=primary_dish_name,
            planned_kg=site.pan_capacity_portions * site.portion_grams / 1000.0,
            sold_kg=round(sold_kg, 1),
            forecast_next_30_min_kg=round(predicted * site.portion_grams / 1000.0, 1),
            pan_level_percent=service_day.pan_fill_percent,
            satisfaction_percent=state_snapshot.get("satisfaction_percent") or 0,
        )
    ]
    existing_names = {primary_dish_name}
    menu_entries_today = get_menu_days_for_date(site_id, service_day.date)
    for menu in menu_entries_today:
        if menu.service_slot == PRIMARY_SERVICE_SLOT:
            continue
        dish_name = menu.main_dish_planned.name
        if dish_name in existing_names:
            continue
        slot_cfg = SLOT_CONFIG_MAP.get(menu.service_slot, {})
        planned_kg = float(slot_cfg.get("planned_kg", 9.0))
        sold_component = sold_kg * float(slot_cfg.get("sold_pct", 0.22))
        forecast_component = predicted * portion_kg * float(slot_cfg.get("forecast_pct", slot_cfg.get("sold_pct", 0.22)))
        pan_level = int(slot_cfg.get("pan_level_base", max(22, service_day.pan_fill_percent - 12)))
        satisfaction_base = slot_cfg.get("satisfaction_base")
        satisfaction = int(satisfaction_base) if satisfaction_base is not None else (state_snapshot.get("satisfaction_percent") or 70)
        live_rows.append(
            LiveDishRow(
                dish_name=dish_name,
                planned_kg=round(planned_kg, 1),
                sold_kg=round(sold_component, 1),
                forecast_next_30_min_kg=round(forecast_component, 1),
                pan_level_percent=max(18, min(95, pan_level)),
                satisfaction_percent=max(45, min(95, satisfaction)),
            )
        )
        existing_names.add(dish_name)
    for template in LIVE_EXTRA_DISHES:
        sold_component = sold_kg * float(template.get("sold_pct", 0.3))
        forecast_component = predicted * portion_kg * float(template.get("forecast_pct", template.get("sold_pct", 0.3)))
        dish_name = str(template.get("dish_name", ""))
        if dish_name in existing_names:
            continue
        live_rows.append(
            LiveDishRow(
                dish_name=dish_name,
                planned_kg=float(template.get("planned_kg", 8.0)),
                sold_kg=round(sold_component, 1),
                forecast_next_30_min_kg=round(forecast_component, 1),
                pan_level_percent=int(template.get("pan_level", max(20, service_day.pan_fill_percent - 12))),
                satisfaction_percent=int(template.get("satisfaction", 70)),
            )
        )
    return live_rows


def build_station_brief(site_id: str, service_day: ServiceDay, feedback_multiplier: float) -> List[Dict[str, object]]:
    menus_today = get_menu_days_for_date(site_id, service_day.date)
    site = get_site_config(site_id)
    rows: List[Dict[str, object]] = []
    if not menus_today:
        return rows
    for menu in menus_today:
        dish = menu.main_dish_planned
        avg_portion = dish.avg_portion_grams or site.portion_grams
        coverage = max(menu.predicted_covers, 1)
        next_wave = max(int(coverage * 0.18 * feedback_multiplier), 1)
        prep_portions = min(next_wave, coverage)
        prep_grams = int(prep_portions * avg_portion)
        signal = "ready"
        detail = "Hold steady"
        if menu.swap_status == "approved":
            detail = f"Serve {menu.recommended_dish.name}"
        elif menu.swap_status == "suggested":
            signal = "warning"
            detail = f"Watch uptake ({int(menu.uptake_if_kept * 100)}%)"
        elif menu.swap_status == "ignored":
            detail = "Keep planned dish"
        if prep_portions >= 60:
            signal = "critical"
            detail = "Big wave incoming – prep extra"
        rows.append(
            {
                "service_slot": menu.service_slot,
                "slot_label": menu.slot_label,
                "station_label": menu.station or menu.slot_label,
                "dish_name": dish.name,
                "dish_type": dish.type,
                "recommended_dish": menu.recommended_dish.name,
                "prep_portions": prep_portions,
                "prep_grams": prep_grams,
                "predicted_diners": prep_portions,
                "signal": signal,
                "detail": detail,
                "swap_status": menu.swap_status,
            }
        )
    rows.sort(key=lambda r: r["slot_label"])
    return rows


def compute_weekly_summary(site_id: str) -> WeeklySummary:
    service_day = get_or_create_service_day(site_id)
    today = service_day.date
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)

    waste_baseline = float(
        service_day.actual_waste_baseline_kg
        or service_day.predicted_waste_if_no_action_kg
        or 40.0
    )
    waste_now = float(
        service_day.actual_waste_kg
        or service_day.predicted_waste_with_flowharmony_kg
        or waste_baseline * 0.35
    )
    waste_now = max(0.0, min(waste_now, waste_baseline))
    avoided_kg = max(0.0, waste_baseline - waste_now)

    co2_saved = float(
        service_day.actual_co2_saved_kg
        or round(avoided_kg * 6.8, 1)
    )
    money_saved = float(
        service_day.actual_money_saved_eur
        or service_day.predicted_waste_savings_eur
        or avoided_kg * 9.8
    )
    bonus_trigger_met = co2_saved >= 100.0 or avoided_kg >= 12.0
    bonus_amount = 0.99 * avoided_kg if bonus_trigger_met else 0.0

    return WeeklySummary(
        site_id=site_id,
        week_start=week_start,
        week_end=week_end,
        waste_kg=round(waste_now, 1),
        waste_baseline_kg=round(waste_baseline, 1),
        co2_saved_kg=round(co2_saved, 1),
        money_saved_eur=round(money_saved, 1),
        bonus_trigger_met=bonus_trigger_met,
        bonus_amount_eur=round(bonus_amount, 2),
    )


def build_wait_time_view(site_id: str) -> Dict[str, object]:
    state_snapshot = compute_current_state(site_id)
    service_day = get_or_create_service_day(site_id)
    menu_entries = get_menu_days_for_date(site_id, service_day.date)
    base_line = max(4, service_day.diners_so_far // 14)
    lines: List[WaitLineView] = []
    for idx, menu in enumerate(menu_entries):
        station = menu.station or "Lunch Line"
        expected_covers = max(menu.predicted_covers, 40)
        variability = (idx % 3) * 1.5
        wait_minutes = (expected_covers / 60.0) + base_line * 0.6 + variability
        if menu.service_slot == PRIMARY_SERVICE_SLOT:
            wait_minutes += 2.0
        wait_minutes = max(2.5, min(wait_minutes, 18.0))
        line_length = int(wait_minutes * 1.4)
        status = "busy" if wait_minutes >= 10 else "steady" if wait_minutes >= 6 else "moving"
        if status == "busy":
            message = "Shift staff to this station or announce alternate line"
        elif status == "steady":
            message = "Line is healthy – keep pans topped up"
        else:
            message = "Great flow – remind diners about seconds"
        lines.append(
            WaitLineView(
                service_slot=menu.service_slot,
                slot_label=menu.slot_label,
                station=station,
                wait_minutes=round(wait_minutes, 1),
                line_length=line_length,
                status=status,
                message=message,
            )
        )
    lines.sort(key=lambda l: l.wait_minutes, reverse=True)
    served_pct = state_snapshot.get("served_percent")
    recent_reactions = get_recent_reactions(service_day)
    return {
        "lines": lines,
        "updated_at": datetime.now().strftime("%H:%M"),
        "served_percent": served_pct,
        "total_expected": state_snapshot.get("total_expected_today"),
        "diners_so_far": state_snapshot.get("diners_so_far"),
        "reactions": recent_reactions,
    }


def build_feedback_summary(site_id: str) -> Dict[str, object]:
    service_day = get_or_create_service_day(site_id)
    entries = get_detailed_feedback_entries(service_day, 200)
    question_stats: Dict[str, Dict[str, object]] = {}
    for entry in entries:
        responses = entry.get("responses", {})
        for key, value in responses.items():
            stats = question_stats.setdefault(key, {
                "counts": {},
                "numeric_total": 0.0,
                "numeric_count": 0,
            })
            str_value = str(value)
            counts = stats["counts"]
            counts[str_value] = counts.get(str_value, 0) + 1
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            stats["numeric_total"] += numeric
            stats["numeric_count"] += 1
    rows: List[FeedbackSummaryRow] = []
    for key, stats in question_stats.items():
        counts: Dict[str, int] = stats["counts"]
        total_responses = sum(counts.values())
        screen = SCREEN_BY_RESPONSE_KEY.get(key)
        option_lookup = {}
        if screen:
            option_lookup = {str(opt.get("value")): opt.get("label") for opt in screen.get("options", [])}
        top_response = None
        if counts:
            raw_top = max(counts.items(), key=lambda item: item[1])[0]
            top_response = option_lookup.get(str(raw_top), str(raw_top))
        avg_score = None
        numeric_count = stats.get("numeric_count", 0)
        if numeric_count:
            avg_score = round(stats["numeric_total"] / numeric_count, 1)
        label = FEEDBACK_LABELS.get(key, key.replace("_", " ").title())
        sorted_counts = sorted(counts.items(), key=lambda item: item[1], reverse=True)
        friendly_breakdown: Dict[str, int] = {}
        for raw_value, count in sorted_counts:
            display_value = option_lookup.get(str(raw_value), str(raw_value))
            friendly_breakdown[display_value] = count
        rows.append(
            FeedbackSummaryRow(
                key=key,
                label=label,
                total_responses=total_responses,
                average_score=avg_score,
                top_response=top_response,
                response_breakdown=friendly_breakdown,
            )
        )
    rows.sort(key=lambda r: r.total_responses, reverse=True)
    recent_entries: List[Dict[str, object]] = []
    for entry in reversed(entries[-6:]):
        responses = entry.get("responses", {})
        if not responses:
            continue
        key, value = next(iter(responses.items()))
        screen = SCREEN_BY_RESPONSE_KEY.get(key)
        label = FEEDBACK_LABELS.get(key, key.replace("_", " ").title())
        pretty_value = str(value)
        if screen:
            option_label = next((opt.get("label") for opt in screen.get("options", []) if str(opt.get("value")) == str(value)), None)
            if option_label:
                pretty_value = option_label
        timestamp_str = entry.get("timestamp")
        display_time = None
        if timestamp_str:
            try:
                display_time = datetime.fromisoformat(timestamp_str).strftime("%H:%M")
            except ValueError:
                display_time = timestamp_str
        recent_entries.append({
            "timestamp": entry.get("timestamp"),
            "display_time": display_time,
            "question": label,
            "value": pretty_value,
            "question_set": entry.get("question_set"),
            "response_key": key,
        })
    return {
        "total_entries": len(entries),
        "rows": rows,
        "recent": recent_entries,
    }


def build_line_alerts(site_id: str) -> List[RedirectAlert]:
    wait_view = build_wait_time_view(site_id)
    alerts: List[RedirectAlert] = []
    for line in wait_view["lines"][:5]:
        if line.wait_minutes < 8:
            continue
        severity = "critical" if line.wait_minutes >= 13 else "warning"
        if severity == "critical":
            action = f"Redirect to {wait_view['lines'][-1].slot_label}"
        else:
            action = "Promote mobile ordering pick-up"
        alerts.append(
            RedirectAlert(
                title=f"{line.slot_label} line is {line.status}",
                subtitle=f"{line.wait_minutes} min wait · {line.line_length} people",
                severity=severity,
                action_text=action,
                target=line.slot_label,
                details=line.message,
            )
        )
    if not alerts:
        alerts.append(
            RedirectAlert(
                title="All lines flowing",
                subtitle="Wait times under 6 minutes",
                severity="info",
                action_text="Keep communicating chef specials",
                target="All stations",
                details="No re-routing needed right now.",
            )
        )
    return alerts


    week_end = week_start + timedelta(days=6)
    service_day = get_or_create_service_day(site_id)
def build_network_alerts(site_id: str) -> List[RedirectAlert]:
    wait_view = build_wait_time_view(site_id)
    busiest_wait = max((line.wait_minutes for line in wait_view["lines"]), default=0)
    alerts: List[RedirectAlert] = []
    for site in CHAIN_SITE_OPTIONS:
        if site["site_id"] == site_id:
            continue
        avg_wait = float(site.get("avg_wait", 7))
        if avg_wait + 1 >= busiest_wait and busiest_wait < 10:
            continue
        delta = busiest_wait - avg_wait
        severity = "critical" if delta >= 5 else "warning"
        alerts.append(
            RedirectAlert(
                title=f"Consider sending diners to {site['name']}",
                subtitle=f"They are running {delta:.0f} min faster",
                severity=severity,
                action_text=f"Offer shuttle / signage to {site['district']}",
                target=site["name"],
                details=f"Avg wait {avg_wait:.0f} min vs {busiest_wait:.0f} min here.",
            )
        )
    if not alerts:
        alerts.append(
            RedirectAlert(
                title="No network re-routing needed",
                subtitle="Nearby SSRs have similar wait times",
                severity="info",
                action_text="Continue local flow optimizations",
                target="Local site",
                details="We will surface re-route suggestions when a gap opens.",
            )
        )
    return alerts


    waste_baseline = float(service_day.actual_waste_baseline_kg or service_day.predicted_waste_if_no_action_kg or 40.0)
    waste_now = float(service_day.actual_waste_kg or service_day.predicted_waste_with_flowharmony_kg or waste_baseline * 0.3)
    waste_now = max(0.0, min(waste_now, waste_baseline))
    avoided_kg = max(0.0, waste_baseline - waste_now)
    co2_saved = float(service_day.actual_co2_saved_kg or round(avoided_kg * 6.8, 1))
    money_saved = float(service_day.actual_money_saved_eur or service_day.predicted_waste_savings_eur or avoided_kg * 8.5)
    bonus_trigger_met = co2_saved > 100 or avoided_kg >= 12
    bonus_amount = 0.99 * avoided_kg if bonus_trigger_met else 0.0
    return WeeklySummary(
        site_id=site_id,
        week_start=week_start,
        week_end=week_end,
        waste_kg=waste_now,
        waste_baseline_kg=waste_baseline,
        co2_saved_kg=co2_saved,
        money_saved_eur=money_saved,
        bonus_trigger_met=bonus_trigger_met,
        bonus_amount_eur=round(bonus_amount, 2),
    )


def build_manager_view(site_id: str) -> ManagerViewModel:
    state_snapshot = compute_current_state(site_id)
    service_day = get_or_create_service_day(site_id)
    today_banner = compute_today_banner(service_day)
    menu_rows = build_menu_rows(site_id)
    alerts = build_alerts(site_id)
    live_rows = build_live_rows(site_id, state_snapshot)
    weekly_report = compute_weekly_summary(site_id)
    return ManagerViewModel(
        today_banner=today_banner,
        menu_prevention_rows=menu_rows,
        alerts_48h=alerts,
        live_overview_rows=live_rows,
        weekly_report=weekly_report,
    )


def update_menu_status(site_id: str, target_date: date, status: str, service_slot: Optional[str] = None) -> None:
    ensure_menu_days_current(site_id)
    updated = False
    for menu in MENU_DAYS.get(site_id, []):
        if menu.date == target_date and (service_slot is None or menu.service_slot == service_slot):
            menu.swap_status = status
            updated = True
    if updated:
        persist_menu_days()


def adjust_pan_after_refill(service_day: ServiceDay, suggested_grams: int, portion_grams: int, capacity_portions: int) -> None:
    portions_added = suggested_grams / portion_grams if suggested_grams else 0
    current_portions = (service_day.pan_fill_percent / 100.0) * capacity_portions
    new_portions = min(capacity_portions, current_portions + portions_added)
    service_day.pan_fill_percent = int((new_portions / capacity_portions) * 100)


def get_matching_decision(service_day: ServiceDay, payload: Dict[str, object]) -> Optional[DecisionCard]:
    card = service_day.last_decision
    if not card:
        return None
    if not payload:
        return card
    wave_idx = payload.get("decision_wave_index")
    if wave_idx is not None and card.wave_index != int(wave_idx):
        return None
    ts = payload.get("decision_timestamp")
    if ts is not None and ts != card.timestamp.isoformat():
        return None
    slot = payload.get("service_slot")
    if slot is not None and card.service_slot != str(slot):
        return None
    return card


# --- Routes ---


@app.route("/")
def index() -> object:
    site_id = get_site_id_from_request()
    site = get_site_config(site_id)
    today_label = datetime.now().strftime("%A %d %b %Y")

    quick_links = [
        {
            "icon": "🍳",
            "label": "Kitchen card",
            "href": url_for("kitchen_view", site_id=site_id),
        },
        {
            "icon": "🪩",
            "label": "Lobby wall",
            "href": url_for("mascot_wall_view", site_id=site_id),
        },
        {
            "icon": "😊",
            "label": "Lunch pulse",
            "href": url_for("feedback_view", site_id=site_id),
        },
        {
            "icon": "🎠",
            "label": "Feedback carousel",
            "href": url_for("feedback_carousel_view", site_id=site_id),
        },
        {
            "icon": "📈",
            "label": "Manager dashboard",
            "href": url_for("manager_view", site_id=site_id),
        },
    ]

    nav_sections = [
        {
            "heading": "Service floor",
            "description": "Live ops tools for the kitchen and front-of-house team.",
            "cards": [
                {
                    "icon": "🍳",
                    "title": "Kitchen card",
                    "description": "Live refill guidance, portions, and wave outlook.",
                    "href": url_for("kitchen_view", site_id=site_id),
                },
                {
                    "icon": "🧮",
                    "title": "Counter tally",
                    "description": "Tap counter to log diners as they arrive.",
                    "href": url_for("counter_view", site_id=site_id),
                },
                {
                    "icon": "🛰️",
                    "title": "Wait board",
                    "description": "Large-screen line status board for the floor crew.",
                    "href": url_for("waitboard_view", site_id=site_id),
                },
                {
                    "icon": "🧭",
                    "title": "Wait tiles",
                    "description": "Tile overview of stations, queues, and actions.",
                    "href": url_for("wait_times_view", site_id=site_id),
                },
                {
                    "icon": "🪩",
                    "title": "Lobby mascot wall",
                    "description": "Full-screen reactions poster with the dancing mascot.",
                    "href": url_for("mascot_wall_view", site_id=site_id),
                },
            ],
        },
        {
            "heading": "Feedback kiosks",
            "description": "Diner-facing emoji panels – ready for instant rotation.",
            "cards": [
                {
                    "icon": "😊",
                    "title": "Lunch pulse",
                    "description": "Single-tap overall satisfaction pulse.",
                    "href": url_for("feedback_view", site_id=site_id),
                },
                {
                    "icon": "👃",
                    "title": "Sensory kiosk",
                    "description": "Smell, appearance, and temperature feedback loop.",
                    "href": url_for("feedback_sensory_view", site_id=site_id),
                },
                {
                    "icon": "🚶",
                    "title": "Flow kiosk",
                    "description": "Wait time and staff warmth check-ins.",
                    "href": url_for("feedback_experience_view", site_id=site_id),
                },
                {
                    "icon": "💶",
                    "title": "Value kiosk",
                    "description": "Portion size and value sentiment panel.",
                    "href": url_for("feedback_value_view", site_id=site_id),
                },
                {
                    "icon": "📊",
                    "title": "Feedback wall",
                    "description": "Manager summary of all feedback streams.",
                    "href": url_for("feedback_all_view", site_id=site_id),
                },
                {
                    "icon": "🎠",
                    "title": "Feedback carousel",
                    "description": "Auto-rotating showcase of every emoji question.",
                    "href": url_for("feedback_carousel_view", site_id=site_id),
                },
            ],
        },
        {
            "heading": "Management radar",
            "description": "Deeper analytics and upcoming menu actions.",
            "cards": [
                {
                    "icon": "📈",
                    "title": "Manager dashboard",
                    "description": "Waste prevention, menu swaps, and weekly KPIs.",
                    "href": url_for("manager_view", site_id=site_id),
                },
                {
                    "icon": "⚠️",
                    "title": "Line alerts",
                    "description": "Station-specific wait advisories and actions.",
                    "href": url_for("line_alerts_view", site_id=site_id),
                },
                {
                    "icon": "🌐",
                    "title": "Network alerts",
                    "description": "Upcoming swap pushes across the estate.",
                    "href": url_for("network_alerts_view", site_id=site_id),
                },
            ],
        },
    ]

    return render_template(
        "home.html",
        site_id=site_id,
        site_name=site.name,
        today_label=today_label,
        quick_links=quick_links,
        nav_sections=nav_sections,
    )


@app.route("/favicon.ico")
def favicon() -> object:
    static_dir = Path(app.static_folder or "static")
    return send_from_directory(static_dir, "favicon.svg", mimetype="image/svg+xml")


@app.route("/kitchen")
def kitchen_view() -> object:
    site_id = get_site_id_from_request()
    site = get_site_config(site_id)
    hide_logo = request.args.get("embed") == "1"
    return render_template("kitchen.html", site_id=site_id, dish_name=site.dish_name, hide_logo=hide_logo)


@app.route("/kitchen_1")
def kitchen_legacy_view() -> object:
    site_id = get_site_id_from_request()
    site = get_site_config(site_id)
    hide_logo = request.args.get("embed") == "1"
    return render_template("kitchen_1.html", site_id=site_id, dish_name=site.dish_name, hide_logo=hide_logo)


@app.route("/feedback")
def feedback_view() -> object:
    site_id = get_site_id_from_request()
    hide_logo = request.args.get("embed") == "1"
    return render_template("feedback.html", site_id=site_id, hide_logo=hide_logo)


@app.route("/counter")
def counter_view() -> object:
    site_id = get_site_id_from_request()
    hide_logo = request.args.get("embed") == "1"
    return render_template("counter.html", site_id=site_id, hide_logo=hide_logo)


@app.route("/waitboard")
def waitboard_view() -> object:
    site_id = get_site_id_from_request()
    view_model = build_wait_time_view(site_id)
    lines_raw = view_model.get("lines", [])
    lines_payload = [asdict(line) for line in lines_raw]
    hide_logo = request.args.get("embed") == "1"
    return render_template(
        "waitboard.html",
        site_id=site_id,
        view=view_model,
        lines=lines_payload,
        lines_json=lines_payload,
        hide_logo=hide_logo,
    )


@app.route("/manager")
def manager_view() -> object:
    site_id = get_site_id_from_request()
    view_model = build_manager_view(site_id)
    hide_logo = request.args.get("embed") == "1"
    return render_template("manager.html", site_id=site_id, view=view_model, hide_logo=hide_logo)


@app.route("/waittimes")
def wait_times_view() -> object:
    site_id = get_site_id_from_request()
    view_model = build_wait_time_view(site_id)
    hide_logo = request.args.get("embed") == "1"
    return render_template("wait_times.html", site_id=site_id, view=view_model, hide_logo=hide_logo)


def render_feedback_template(template_name: str, route_key: str) -> object:
    site_id = get_site_id_from_request()
    requested_slug = request.args.get("slug")
    default_slug = DEFAULT_SCREEN_FOR_ROUTE.get(route_key)
    slug = requested_slug or default_slug
    screen = SCREEN_BY_SLUG.get(slug)
    if not screen:
        abort(404)
    siblings = [candidate for candidate in FEEDBACK_SCREENS if candidate.get("question_set") == screen.get("question_set")]
    hide_logo = request.args.get("embed") == "1"
    return render_template(
        f"{template_name}.html",
        site_id=site_id,
        screen=screen,
        sibling_screens=siblings,
        route_map=QUESTION_SET_TO_ROUTE,
        hide_logo=hide_logo,
    )


@app.route("/feedback1")
def feedback_sensory_view() -> object:
    return render_feedback_template("feedback1", "feedback1")


@app.route("/feedback2")
def feedback_experience_view() -> object:
    return render_feedback_template("feedback2", "feedback2")


@app.route("/feedback3")
def feedback_value_view() -> object:
    return render_feedback_template("feedback3", "feedback3")


@app.route("/feedback_all")
def feedback_all_view() -> object:
    site_id = get_site_id_from_request()
    summary = build_feedback_summary(site_id)
    embeds: List[Dict[str, object]] = []
    for question_set, route in QUESTION_SET_TO_ROUTE.items():
        screen = next((candidate for candidate in FEEDBACK_SCREENS if candidate.get("question_set") == question_set), None)
        if not screen:
            continue
        embeds.append({
            "route": route,
            "slug": screen.get("slug"),
            "title": screen.get("title"),
        })
    hide_logo = request.args.get("embed") == "1"
    return render_template(
        "feedback_all.html",
        site_id=site_id,
        summary=summary,
        screens=FEEDBACK_SCREENS,
        embeds=embeds,
        route_map=QUESTION_SET_TO_ROUTE,
        hide_logo=hide_logo,
    )


@app.route("/mascot_wall")
def mascot_wall_view() -> object:
    site_id = get_site_id_from_request()
    summary = build_feedback_summary(site_id)
    hide_logo = request.args.get("embed") == "1"
    return render_template("mascot_wall.html", site_id=site_id, summary=summary, hide_logo=hide_logo)


@app.route("/feedback_carousel")
def feedback_carousel_view() -> object:
    site_id = get_site_id_from_request()
    hide_logo = request.args.get("embed") == "1"
    return render_template(
        "feedback_carousel.html",
        site_id=site_id,
        screens=FEEDBACK_SCREENS,
        hide_logo=hide_logo,
    )


@app.route("/line_alerts")
def line_alerts_view() -> object:
    site_id = get_site_id_from_request()
    alerts = build_line_alerts(site_id)
    hide_logo = request.args.get("embed") == "1"
    return render_template("line_alerts.html", site_id=site_id, alerts=alerts, hide_logo=hide_logo)


@app.route("/network_alerts")
def network_alerts_view() -> object:
    site_id = get_site_id_from_request()
    alerts = build_network_alerts(site_id)
    hide_logo = request.args.get("embed") == "1"
    return render_template("network_alerts.html", site_id=site_id, alerts=alerts, hide_logo=hide_logo)


@app.route("/api/state")
def api_state() -> object:
    site_id = get_site_id_from_request()
    state_snapshot = compute_current_state(site_id)
    return jsonify(state_snapshot)


@app.route("/api/waits")
def api_waits() -> object:
    site_id = get_site_id_from_request()
    view = build_wait_time_view(site_id)
    response = dict(view)
    response["lines"] = [asdict(line) for line in view.get("lines", [])]
    response["reactions"] = view.get("reactions", [])
    return jsonify(response)


@app.route("/api/feedback_summary")
def api_feedback_summary() -> object:
    site_id = get_site_id_from_request()
    summary = build_feedback_summary(site_id)
    return jsonify(summary)


@app.route("/api/reactions")
def api_reactions() -> object:
    site_id = get_site_id_from_request()
    service_day = get_or_create_service_day(site_id)
    return jsonify({"reactions": get_recent_reactions(service_day)})


@app.route("/api/increment_diner", methods=["POST"])
def api_increment_diner() -> object:
    site_id = get_site_id_from_request()
    service_day = get_or_create_service_day(site_id)
    data = request.get_json(silent=True) or {}
    delta = int(data.get("delta", 1))
    service_day.diners_so_far = max(service_day.diners_so_far + delta, 0)
    return jsonify({"ok": True, "diners_so_far": service_day.diners_so_far})


@app.route("/api/feedback", methods=["POST"])
def api_feedback() -> object:
    site_id = get_site_id_from_request()
    service_day = get_or_create_service_day(site_id)
    data = request.get_json(force=True)
    rating = int(data.get("rating", 0))
    if rating in (1, 2, 3):
        service_day.feedback_scores.append(rating)
        service_day.feedback_scores[:] = service_day.feedback_scores[-100:]
        _persist_feedback_score(service_day, rating)
        reaction_meta = LUNCH_PULSE_REACTIONS.get(str(rating), {})
        append_reaction_entry(
            service_day,
            response_key="lunch_pulse",
            value=rating,
            question_set="lunch_pulse",
            title="Lunch pulse",
            label=reaction_meta.get("label"),
            emoji=reaction_meta.get("emoji"),
            source="quick",
        )
        return jsonify({"ok": True, "rating": rating})
    return jsonify({"ok": False, "error": "invalid rating"}), 400


@app.route("/api/feedback_extended", methods=["POST"])
def api_feedback_extended() -> object:
    site_id = get_site_id_from_request()
    service_day = get_or_create_service_day(site_id)
    payload = request.get_json(force=True)
    responses = payload.get("responses")
    if not isinstance(responses, dict) or not responses:
        return jsonify({"ok": False, "error": "missing responses"}), 400
    entry = {
        "question_set": str(payload.get("question_set", "general")),
        "dish": payload.get("dish"),
        "timestamp": datetime.utcnow().isoformat(),
        "responses": responses,
    }
    service_day.detailed_feedback.append(entry)
    service_day.detailed_feedback[:] = service_day.detailed_feedback[-400:]
    _persist_detailed_feedback(service_day, entry)
    question_set = entry["question_set"]
    for key, value in responses.items():
        screen = SCREEN_BY_RESPONSE_KEY.get(str(key))
        append_reaction_entry(
            service_day,
            response_key=str(key),
            value=value,
            question_set=(screen or {}).get("question_set") or question_set,
            title=(screen or {}).get("title"),
            source="extended",
        )
    total_entries = len(get_detailed_feedback_entries(service_day, DETAILED_FEEDBACK_HISTORY_LIMIT))
    return jsonify({"ok": True, "total": total_entries})


@app.route("/api/done", methods=["POST"])
def api_done() -> object:
    site_id = get_site_id_from_request()
    site = get_site_config(site_id)
    service_day = get_or_create_service_day(site_id)
    payload = request.get_json(silent=True) or {}
    card = get_matching_decision(service_day, payload)
    if not card:
        return jsonify({"ok": False, "error": "stale decision"}), 409
    adjust_pan_after_refill(service_day, card.suggested_grams, site.portion_grams, site.pan_capacity_portions)
    card.status = "done"
    return jsonify({"ok": True, "pan_fill_percent": service_day.pan_fill_percent})


@app.route("/api/skip", methods=["POST"])
def api_skip() -> object:
    site_id = get_site_id_from_request()
    service_day = get_or_create_service_day(site_id)
    payload = request.get_json(silent=True) or {}
    card = get_matching_decision(service_day, payload)
    if not card:
        return jsonify({"ok": False, "error": "stale decision"}), 409
    card.status = "skipped"
    return jsonify({"ok": True, "status": card.status})


@app.route("/api/manager/send_menu_to_jamix", methods=["POST"])
def api_manager_send_menu_to_jamix() -> object:
    site_id = get_site_id_from_request()
    payload = request.get_json(silent=True) or {}
    day_str = payload.get("date")
    service_slot = payload.get("service_slot")
    if day_str:
        target = datetime.fromisoformat(day_str).date()
        update_menu_status(site_id, target, "approved", service_slot)
    return jsonify({"ok": True})


@app.route("/api/manager/approve_swap", methods=["POST"])
def api_manager_approve_swap() -> object:
    site_id = get_site_id_from_request()
    payload = request.get_json(force=True)
    target = datetime.fromisoformat(payload["date"]).date()
    service_slot = payload.get("service_slot")
    update_menu_status(site_id, target, "approved", service_slot)
    return jsonify({"ok": True})


@app.route("/api/manager/ignore_swap", methods=["POST"])
def api_manager_ignore_swap() -> object:
    site_id = get_site_id_from_request()
    payload = request.get_json(force=True)
    target = datetime.fromisoformat(payload["date"]).date()
    service_slot = payload.get("service_slot")
    update_menu_status(site_id, target, "ignored", service_slot)
    return jsonify({"ok": True})


@app.route("/api/reset_day", methods=["POST"])
def api_reset_day() -> object:
    site_id = get_site_id_from_request()
    key = (site_id, date.today())
    service_day = SERVICE_DAYS.pop(key, None)
    if service_day is None:
        service_day = get_or_create_service_day(site_id)
    _clear_service_day_storage(service_day)
    SERVICE_DAYS.pop(key, None)
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
