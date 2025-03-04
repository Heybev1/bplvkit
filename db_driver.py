import sqlite3
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from contextlib import contextmanager
import json
import os
from datetime import datetime

@dataclass
class Bev:
    id: str
    name: str
    category: str
    subcategory: str
    price: int
    inventory: int
    image: str
    sales: Optional[int] = 0
    category_id: Optional[int] = None

class DatabaseDriver:
    def __init__(self, db_path: str = "auto_db.sqlite"):
        self.db_path = db_path
        self._init_db()
        if not self._has_data():
            self._load_initial_data()

    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Create bevs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bevs (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    subcategory TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    inventory INTEGER NOT NULL,
                    image TEXT NOT NULL,
                    sales INTEGER DEFAULT 0,
                    supplier_id INTEGER,
                    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
                )
            """)

            # Create suppliers table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS suppliers (
                    supplier_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    contact_info TEXT,
                    address TEXT
                )
            """)

            # Create transactions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_date DATE NOT NULL,
                    transaction_time TIME NOT NULL,
                    total_amount REAL NOT NULL,
                    tax_amount REAL NOT NULL,
                    payment_method TEXT NOT NULL,
                    employee_id INTEGER
                )
            """)

            # Create transaction_items table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transaction_items (
                    transaction_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id INTEGER NOT NULL,
                    item_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    unit_price REAL NOT NULL,
                    line_total REAL NOT NULL,
                    tax_category TEXT NOT NULL,
                    tax_amount REAL NOT NULL,
                    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
                    FOREIGN KEY (item_id) REFERENCES bevs(id)
                )
            """)

            # Create tax_rates table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tax_rates (
                    tax_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tax_type TEXT NOT NULL,
                    rate REAL NOT NULL,
                    description TEXT
                )
            """)

            # Create tax_details table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tax_details (
                    tax_detail_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_item_id INTEGER NOT NULL,
                    tax_id INTEGER NOT NULL,
                    calculated_tax_amount REAL NOT NULL,
                    FOREIGN KEY (transaction_item_id) REFERENCES transaction_items(transaction_item_id),
                    FOREIGN KEY (tax_id) REFERENCES tax_rates(tax_id)
                )
            """)

            # Create revenue_summary table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS revenue_summary (
                    revenue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE NOT NULL,
                    shift TEXT NOT NULL,
                    total_sales REAL NOT NULL,
                    total_tax REAL NOT NULL,
                    number_of_transactions INTEGER NOT NULL
                )
            """)

            # Create events table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_name TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_date DATE NOT NULL,
                    event_time TIME NOT NULL,
                    venue TEXT,
                    description TEXT,
                    client_id INTEGER,
                    status TEXT DEFAULT 'pending'
                )
            """)

            # Create event_bookings table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS event_bookings (
                    booking_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    service_type TEXT NOT NULL,
                    service_details TEXT,
                    cost REAL NOT NULL,
                    drink_package TEXT,
                    FOREIGN KEY (event_id) REFERENCES events(event_id)
                )
            """)

            # Create customer_profiles table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customer_profiles (
                    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    email TEXT UNIQUE,
                    phone TEXT UNIQUE,
                    preferences TEXT,
                    visit_count INTEGER DEFAULT 0,
                    last_visit DATE
                )
            """)

            # Create order_batches table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS order_batches (
                    batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    table_number TEXT,
                    status TEXT DEFAULT 'pending',
                    customer_id INTEGER,
                    FOREIGN KEY (customer_id) REFERENCES customer_profiles(customer_id)
                )
            """)

            # Create batch_items table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS batch_items (
                    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL,
                    bev_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    notes TEXT,
                    status TEXT DEFAULT 'pending',
                    FOREIGN KEY (batch_id) REFERENCES order_batches(batch_id),
                    FOREIGN KEY (bev_id) REFERENCES bevs(id)
                )
            """)
            
            # Create drink_recommendations table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS drink_recommendations (
                    recommendation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bev_id TEXT NOT NULL,
                    recommended_bev_id TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    FOREIGN KEY (bev_id) REFERENCES bevs(id),
                    FOREIGN KEY (recommended_bev_id) REFERENCES bevs(id)
                )
            """)
            
            # Create indexes for better query performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transaction_items_transaction ON transaction_items(transaction_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_batch_items_batch ON batch_items(batch_id)")
            
            conn.commit()

    def _migrate_existing_table(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Add category column to existing table if it doesn't exist
            try:
                cursor.execute("""
                    ALTER TABLE bevs 
                    ADD COLUMN category TEXT DEFAULT 'Uncategorized'
                """)
                conn.commit()
            except Exception as e:
                # Column might already exist
                pass

    def _has_data(self) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM bevs")
            count = cursor.fetchone()[0]
            return count > 0

    def _generate_id(self, name: str) -> str:
        return name.lower().replace(" ", "_")

    def _load_initial_data(self):
        json_path = os.path.join(os.path.dirname(__file__), "drinks.json")
        if not os.path.exists(json_path):
            return

        with open(json_path, 'r') as f:
            drinks = json.load(f)

        for drink in drinks:
            id = self._generate_id(drink['name'])
            self.create_bev(
                id=id,
                name=drink['name'],
                category=drink['category'],
                subcategory=drink['subcategory'],
                price=drink['price'],
                inventory=drink['inventory'],
                image="",  # Default empty image URL
                sales=drink.get('sales', 0)
            )

    def _bev_exists(self, id: str) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM bevs WHERE id = ?", (id,))
            return cursor.fetchone() is not None

    def create_bev(self, id: str, name: str, category: str, subcategory: str, price: int, inventory: int, image: str, sales: int = 0) -> Bev:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if self._bev_exists(id):
                # Update existing record
                cursor.execute("""
                    UPDATE bevs 
                    SET name=?, category=?, subcategory=?, price=?, inventory=?, image=?, sales=?
                    WHERE id=?
                """, (name, category, subcategory, price, inventory, image, sales, id))
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO bevs (id, name, category, subcategory, price, inventory, image, sales) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (id, name, category, subcategory, price, inventory, image, sales))
            conn.commit()
            return Bev(id=id, name=name, category=category, subcategory=subcategory, 
                      price=price, inventory=inventory, image=image, sales=sales)

    def get_bev_by_id(self, id: str) -> Optional[Bev]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM bevs WHERE id = ?", (id,))
            row = cursor.fetchone()
            if not row:
                return None
            
            return Bev(
                id=row[0],
                name=row[1],
                category=row[2],
                subcategory=row[3],
                price=row[4],
                inventory=row[5],
                image=row[6],
                sales=row[7]
            )

    def initialize_tax_rates(self):
        """Initialize default tax rates if not exists"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # First check if we have tax rates
            cursor.execute("SELECT COUNT(*) FROM tax_rates")
            if cursor.fetchone()[0] == 0:
                # Insert default tax rates
                default_rates = [
                    ('pour/shot', 0.07, 'Standard tax for individual pours/shots'),
                    ('glass', 0.07, 'Standard tax for glass service'),
                    ('bottle', 0.09, 'Higher tax rate for bottle service'),
                    ('event', 0.10, 'Special tax rate for event packages')
                ]
                cursor.executemany(
                    "INSERT INTO tax_rates (tax_type, rate, description) VALUES (?, ?, ?)",
                    default_rates
                )
                conn.commit()

    def calculate_item_tax(self, item_id: str, quantity: int, tax_category: str) -> Tuple[float, float]:
        """Calculate tax for a transaction item"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Get the item price
            cursor.execute("SELECT price FROM bevs WHERE id = ?", (item_id,))
            price_row = cursor.fetchone()
            if not price_row:
                return 0.0, 0.0
                
            unit_price = price_row[0]
            line_total = unit_price * quantity
            
            # Get the tax rate for this category
            cursor.execute("SELECT rate FROM tax_rates WHERE tax_type = ?", (tax_category,))
            tax_row = cursor.fetchone()
            tax_rate = tax_row[0] if tax_row else 0.07  # Default to 7% if no specific rate
            
            tax_amount = line_total * tax_rate
            
            return line_total, tax_amount

    def record_tax_detail(self, transaction_item_id: int, tax_category: str, tax_amount: float):
        """Record detailed tax information for a transaction item"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Get tax_id for the category
            cursor.execute("SELECT tax_id FROM tax_rates WHERE tax_type = ?", (tax_category,))
            tax_row = cursor.fetchone()
            if not tax_row:
                return
                
            tax_id = tax_row[0]
            
            # Record tax detail
            cursor.execute("""
                INSERT INTO tax_details (transaction_item_id, tax_id, calculated_tax_amount)
                VALUES (?, ?, ?)
            """, (transaction_item_id, tax_id, tax_amount))
            
            conn.commit()

    def create_transaction(self, payment_method: str, items: list, employee_id: Optional[int] = None) -> int:
        """Create a new transaction with items and tax tracking"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now()
            
            # Format date and time as strings for SQLite
            date_str = now.date().isoformat()
            time_str = now.time().isoformat()
            
            # Create transaction
            cursor.execute("""
                INSERT INTO transactions (
                    transaction_date, transaction_time, total_amount, 
                    tax_amount, payment_method, employee_id
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                date_str, time_str, 0, 0, payment_method, employee_id
            ))
            
            transaction_id = cursor.lastrowid
            total_amount = 0
            total_tax = 0
            
            # Process items with tax calculations
            for item in items:
                bev_id = item['id']
                quantity = item['quantity']
                tax_category = item.get('tax_category', 'pour/shot')  # Default to pour/shot
                
                line_total, tax_amount = self.calculate_item_tax(bev_id, quantity, tax_category)
                
                # Create transaction item
                cursor.execute("""
                    INSERT INTO transaction_items (
                        transaction_id, item_id, quantity, unit_price,
                        line_total, tax_category, tax_amount
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    transaction_id, bev_id, quantity,
                    line_total/quantity if quantity > 0 else 0, line_total,
                    tax_category, tax_amount
                ))
                
                # Record tax detail
                self.record_tax_detail(cursor.lastrowid, tax_category, tax_amount)
                
                total_amount += line_total
                total_tax += tax_amount
            
            # Update transaction totals
            cursor.execute("""
                UPDATE transactions 
                SET total_amount = ?, tax_amount = ?
                WHERE transaction_id = ?
            """, (total_amount, total_tax, transaction_id))
            
            conn.commit()
            return transaction_id

    def delete_bev(self, id: str) -> bool:
        """Delete a beverage from the database"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM bevs WHERE id = ?", (id,))
            conn.commit()
            return cursor.rowcount > 0

    def get_bevs_by_category(self, category: str) -> List[Bev]:
        """Get all beverages in a category"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM bevs WHERE category = ?", (category,))
            rows = cursor.fetchall()
            return [Bev(
                id=row[0],
                name=row[1],
                category=row[2],
                subcategory=row[3],
                price=row[4],
                inventory=row[5],
                image=row[6],
                sales=row[7]
            ) for row in rows]

    def create_event(self, name: str, event_type: str, date: str, time: str, 
                    venue: str, client_id: Optional[int] = None, 
                    description: Optional[str] = None) -> Optional[int]:
        """Create a new event"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO events (event_name, event_type, event_date, event_time, 
                                  venue, client_id, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (name, event_type, date, time, venue, client_id, description))
            conn.commit()
            return cursor.lastrowid

    def create_event_booking(self, event_id: int, service_type: str, 
                           drink_package: str, cost: float, 
                           details: Optional[str] = None) -> Optional[int]:
        """Create a new event booking"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO event_bookings (event_id, service_type, drink_package, 
                                          service_details, cost)
                VALUES (?, ?, ?, ?, ?)
            """, (event_id, service_type, drink_package, details, cost))
            conn.commit()
            return cursor.lastrowid

    def get_event_details(self, event_id: int) -> Optional[dict]:
        """Get event details including bookings"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT e.*, eb.service_type, eb.drink_package, eb.cost 
                FROM events e
                LEFT JOIN event_bookings eb ON e.event_id = eb.event_id
                WHERE e.event_id = ?
            """, (event_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "event_id": row[0],
                "name": row[1],
                "type": row[2],
                "date": row[3],
                "time": row[4],
                "venue": row[5],
                "description": row[6],
                "client_id": row[7],
                "status": row[8],
                "drink_package": row[10],
                "package_cost": row[11]
            }

    def get_revenue_summary(self, date: Optional[str] = None, 
                          shift: Optional[str] = None) -> Optional[dict]:
        """Get revenue summary for a date/shift"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM revenue_summary WHERE 1=1"
            params = []
            if date:
                query += " AND date = ?"
                params.append(date)
            if shift:
                query += " AND shift = ?"
                params.append(shift)
            
            cursor.execute(query, params)
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "date": row[1],
                "shift": row[2],
                "total_sales": row[3],
                "total_tax": row[4],
                "transactions": row[5]
            }

    def generate_receipt(self, transaction_id: int) -> Dict[str, Any]:
        """Generate a detailed receipt for a transaction"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Get transaction details
            cursor.execute("""
                SELECT t.transaction_id, t.transaction_date, t.transaction_time, 
                       t.total_amount, t.tax_amount, t.payment_method
                FROM transactions t
                WHERE t.transaction_id = ?
            """, (transaction_id,))
            
            transaction = cursor.fetchone()
            if not transaction:
                return {"error": "Transaction not found"}
            
            # Get transaction items
            cursor.execute("""
                SELECT ti.item_id, b.name, ti.quantity, ti.unit_price, 
                       ti.line_total, ti.tax_amount
                FROM transaction_items ti
                JOIN bevs b ON ti.item_id = b.id
                WHERE ti.transaction_id = ?
            """, (transaction_id,))
            
            items = cursor.fetchall()
            
            receipt = {
                "transaction_id": transaction[0],
                "date": transaction[1],
                "time": transaction[2],
                "items": [
                    {
                        "name": item[1],
                        "quantity": item[2],
                        "unit_price": item[3],
                        "total": item[4],
                        "tax": item[5]
                    } for item in items
                ],
                "subtotal": transaction[3] - transaction[4],
                "tax": transaction[4],
                "total": transaction[3],
                "payment_method": transaction[5]
            }
            
            return receipt
    
    def create_batch_order(self, table_number: str, customer_id: Optional[int] = None) -> int:
        """Create a new batch order"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO order_batches (table_number, customer_id)
                VALUES (?, ?)
            """, (table_number, customer_id))
            conn.commit()
            return cursor.lastrowid
    
    def add_to_batch(self, batch_id: int, bev_id: str, quantity: int, notes: Optional[str] = None) -> bool:
        """Add an item to a batch order"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO batch_items (batch_id, bev_id, quantity, notes)
                VALUES (?, ?, ?, ?)
            """, (batch_id, bev_id, quantity, notes))
            conn.commit()
            return True
    
    def get_batch_order(self, batch_id: int) -> Dict[str, Any]:
        """Get a batch order with all its items"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Get batch details
            cursor.execute("""
                SELECT batch_id, order_time, table_number, status, customer_id
                FROM order_batches
                WHERE batch_id = ?
            """, (batch_id,))
            
            batch = cursor.fetchone()
            if not batch:
                return {"error": "Batch order not found"}
            
            # Get batch items
            cursor.execute("""
                SELECT bi.item_id, b.name, bi.quantity, b.price, bi.notes, bi.status
                FROM batch_items bi
                JOIN bevs b ON bi.bev_id = b.id
                WHERE bi.batch_id = ?
            """, (batch_id,))
            
            items = cursor.fetchall()
            
            # Calculate totals
            subtotal = sum([item[2] * item[3] for item in items])  # quantity * price
            tax_rate = 0.07  # Default tax rate
            tax = subtotal * tax_rate
            total = subtotal + tax
            
            return {
                "batch_id": batch[0],
                "order_time": batch[1],
                "table_number": batch[2],
                "status": batch[3],
                "customer_id": batch[4],
                "items": [
                    {
                        "id": item[0],
                        "name": item[1],
                        "quantity": item[2],
                        "unit_price": item[3],
                        "notes": item[4],
                        "status": item[5]
                    } for item in items
                ],
                "subtotal": subtotal,
                "tax": tax,
                "total": total
            }
    
    def process_batch_to_transaction(self, batch_id: int, payment_method: str) -> int:
        """Process a batch order into a transaction"""
        batch = self.get_batch_order(batch_id)
        if "error" in batch:
            return -1
        
        # Create transaction items from batch
        items = []
        for item in batch["items"]:
            items.append({
                "id": item["id"],
                "quantity": item["quantity"],
                "tax_category": "pour/shot"  # Default tax category
            })
        
        # Create transaction
        transaction_id = self.create_transaction(payment_method, items)
        
        # Update batch status
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE order_batches 
                SET status = 'completed' 
                WHERE batch_id = ?
            """, (batch_id,))
            conn.commit()
        
        return transaction_id
    
    def get_recommendations(self, bev_id: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Get beverage recommendations based on a current beverage"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # First check for explicit recommendations
            cursor.execute("""
                SELECT r.recommended_bev_id, b.name, b.category, b.price, r.confidence
                FROM drink_recommendations r
                JOIN bevs b ON r.recommended_bev_id = b.id
                WHERE r.bev_id = ?
                ORDER BY r.confidence DESC
                LIMIT ?
            """, (bev_id, limit))
            
            recommendations = cursor.fetchall()
            
            # If no explicit recommendations, find similar beverages in the same category
            if not recommendations:
                # Get the beverage's category and subcategory
                cursor.execute("""
                    SELECT category, subcategory
                    FROM bevs
                    WHERE id = ?
                """, (bev_id,))
                
                bev_data = cursor.fetchone()
                if not bev_data:
                    return []
                
                # Find beverages in the same category/subcategory
                cursor.execute("""
                    SELECT id, name, category, price, inventory
                    FROM bevs
                    WHERE category = ? AND subcategory = ? AND id != ?
                    ORDER BY sales DESC
                    LIMIT ?
                """, (bev_data[0], bev_data[1], bev_id, limit))
                
                similar_bevs = cursor.fetchall()
                
                recommendations = [(bev[0], bev[1], bev[2], bev[3], 0.7) for bev in similar_bevs]
            
            return [
                {
                    "id": rec[0],
                    "name": rec[1],
                    "category": rec[2],
                    "price": rec[3],
                    "confidence": rec[4]
                } for rec in recommendations
            ]
    
    def add_recommendation(self, bev_id: str, recommended_bev_id: str, confidence: float) -> bool:
        """Add or update a recommendation between two beverages"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if recommendation already exists
            cursor.execute("""
                SELECT recommendation_id
                FROM drink_recommendations
                WHERE bev_id = ? AND recommended_bev_id = ?
            """, (bev_id, recommended_bev_id))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing recommendation
                cursor.execute("""
                    UPDATE drink_recommendations
                    SET confidence = ?
                    WHERE bev_id = ? AND recommended_bev_id = ?
                """, (confidence, bev_id, recommended_bev_id))
            else:
                # Create new recommendation
                cursor.execute("""
                    INSERT INTO drink_recommendations (bev_id, recommended_bev_id, confidence)
                    VALUES (?, ?, ?)
                """, (bev_id, recommended_bev_id, confidence))
            
            conn.commit()
            return True
    
    def get_sales_trend(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get sales trend data for the specified number of days"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT transaction_date, 
                       COUNT(*) as transaction_count, 
                       SUM(total_amount) as sales_total,
                       SUM(tax_amount) as tax_total
                FROM transactions 
                WHERE transaction_date >= date('now', ?)
                GROUP BY transaction_date
                ORDER BY transaction_date
            """, (f"-{days} days",))
            
            rows = cursor.fetchall()
            return [{
                "date": row[0],
                "transaction_count": row[1],
                "sales_total": row[2],
                "tax_total": row[3]
            } for row in rows]
    
    def get_popular_items(self, category: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the most popular items based on sales"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            query = """
                SELECT b.id, b.name, b.category, b.subcategory, 
                       b.price, b.sales, COUNT(ti.item_id) as order_count
                FROM bevs b
                LEFT JOIN transaction_items ti ON b.id = ti.item_id
            """
            
            params = []
            if category:
                query += " WHERE b.category = ?"
                params.append(category)
            
            query += """
                GROUP BY b.id
                ORDER BY order_count DESC, b.sales DESC
                LIMIT ?
            """
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            return [{
                "id": row[0],
                "name": row[1],
                "category": row[2],
                "subcategory": row[3],
                "price": row[4],
                "sales": row[5],
                "order_count": row[6]
            } for row in rows]

    def finalize_batch(self, batch_id: int, payment_method: str = "cash") -> bool:
        """
        Finalize a batch order and process payment
        
        Args:
            batch_id: The ID of the batch to finalize
            payment_method: Payment method (cash/card)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get batch details
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT items.bev_id, items.quantity, beverages.price, beverages.inventory 
                FROM batch_items items
                JOIN beverages ON items.bev_id = beverages.id
                WHERE items.batch_id = ?
            """, (batch_id,))
            items = cursor.fetchall()
            
            if not items:
                return False
                
            # Calculate total
            total = sum(quantity * price for _, quantity, price, _ in items)
            
            # Update inventory and mark batch as paid
            cursor.execute("BEGIN TRANSACTION")
            try:
                for bev_id, quantity, _, curr_inventory in items:
                    new_inventory = curr_inventory - quantity
                    if new_inventory < 0:
                        cursor.execute("ROLLBACK")
                        return False
                    cursor.execute("""
                        UPDATE beverages 
                        SET inventory = ?, 
                            sales = sales + ?
                        WHERE id = ?
                    """, (new_inventory, quantity, bev_id))
                
                cursor.execute("""
                    UPDATE batches 
                    SET status = 'paid',
                        payment_method = ?,
                        total = ?
                    WHERE id = ?
                """, (payment_method, total, batch_id))
                
                cursor.execute("COMMIT")
                return True
                
            except Exception as e:
                cursor.execute("ROLLBACK")
                raise e
                
        except Exception as e:
            print(f"Error finalizing batch: {e}")
            return False

    def cancel_batch(self, batch_id: int) -> bool:
        """
        Cancel a batch order
        
        Args:
            batch_id: The ID of the batch to cancel
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                UPDATE batches 
                SET status = 'cancelled'
                WHERE id = ? AND status = 'pending'
            """, (batch_id,))
            self.conn.commit()
            return cursor.rowcount > 0
            
        except Exception as e:
            print(f"Error cancelling batch: {e}")
            return False