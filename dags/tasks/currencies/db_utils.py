from datetime import date, timedelta
from db.connection import get_connection
from configs.config import START_DATE, CURRENCY_TABLE

def get_start_date_from_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(date) FROM prod.{CURRENCY_TABLE};")
            result = cur.fetchone()[0]
            if result and result < date.today():
                return (result + timedelta(days=1)).strftime("%Y-%m-%d")
            return result.strftime("%Y-%m-%d") if result else START_DATE