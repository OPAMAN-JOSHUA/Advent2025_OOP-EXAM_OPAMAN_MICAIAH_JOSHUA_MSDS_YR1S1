# price_tracker.py
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple
import pandas as pd
import matplotlib.pyplot as plt

DB_SCHEMA = {
    "products": """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
    """,
    "markets": """
        CREATE TABLE IF NOT EXISTS markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            location TEXT
        );
    """,
    "prices": """
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            market_id INTEGER NOT NULL,
            price REAL NOT NULL,
            date TEXT NOT NULL,
            FOREIGN KEY(product_id) REFERENCES products(id),
            FOREIGN KEY(market_id) REFERENCES markets(id)
        );
    """
}

@dataclass
class Product:
    name: str

@dataclass
class Market:
    name: str
    location: str = ""

@dataclass
class PriceRecord:
    product: str
    market: str
    price: float
    date: str  # ISO format YYYY-MM-DD

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._connect() as con:
            cur = con.cursor()
            for ddl in DB_SCHEMA.values():
                cur.execute(ddl)
            con.commit()

    def add_product(self, name: str):
        with self._connect() as con:
            con.execute("INSERT OR IGNORE INTO products(name) VALUES (?)", (name,))
            con.commit()

    def add_market(self, name: str, location: str = ""):
        with self._connect() as con:
            con.execute("INSERT OR IGNORE INTO markets(name, location) VALUES (?, ?)", (name, location))
            con.commit()

    def get_product_id(self, name: str) -> Optional[int]:
        with self._connect() as con:
            cur = con.execute("SELECT id FROM products WHERE name = ?", (name,))
            row = cur.fetchone()
            return row[0] if row else None

    def get_market_id(self, name: str) -> Optional[int]:
        with self._connect() as con:
            cur = con.execute("SELECT id FROM markets WHERE name = ?", (name,))
            row = cur.fetchone()
            return row[0] if row else None

    def record_price(self, product: str, market: str, price: float, date_iso: str):
        pid = self.get_product_id(product)
        mid = self.get_market_id(market)
        if pid is None or mid is None:
            raise ValueError("Unknown product or market. Ensure they are added first.")
        with self._connect() as con:
            con.execute(
                "INSERT INTO prices(product_id, market_id, price, date) VALUES (?, ?, ?, ?)",
                (pid, mid, price, date_iso)
            )
            con.commit()

    def fetch_timeseries(self, product: str, market: Optional[str] = None) -> pd.DataFrame:
        query = (
            "SELECT p.name as product, m.name as market, pr.price, pr.date "
            "FROM prices pr "
            "JOIN products p ON pr.product_id = p.id "
            "JOIN markets m ON pr.market_id = m.id "
            "WHERE p.name = ?"
        )
        params = [product]
        if market:
            query += " AND m.name = ?"
            params.append(market)
        query += " ORDER BY pr.date ASC"
        with self._connect() as con:
            df = pd.read_sql_query(query, con, params=params)
        df['date'] = pd.to_datetime(df['date'])
        return df

    def list_products(self) -> List[str]:
        with self._connect() as con:
            return [r[0] for r in con.execute("SELECT name FROM products ORDER BY name").fetchall()]

    def list_markets(self) -> List[Tuple[str, str]]:
        with self._connect() as con:
            return [(r[0], r[1]) for r in con.execute("SELECT name, location FROM markets ORDER BY name").fetchall()]

class Plotter:
    def __init__(self, db: DatabaseManager):
        self.db = db

    def plot_product_trend(self, product: str, market: Optional[str] = None, out_path: Optional[str] = None):
        df = self.db.fetch_timeseries(product, market)
        if df.empty:
            raise ValueError("No data to plot.")
        plt.figure(figsize=(8,4.5))
        if market:
            plt.plot(df['date'], df['price'], marker='o', label=f"{product} - {market}")
        else:
            for mk, grp in df.groupby('market'):
                plt.plot(grp['date'], grp['price'], marker='o', label=mk)
        plt.title(f"Price Trend: {product}" + (f" ({market})" if market else ""))
        plt.xlabel("Date")
        plt.ylabel("Price (UGX per kg)")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        if out_path:
            plt.savefig(out_path, dpi=150)
            plt.close()
        else:
            plt.show()

