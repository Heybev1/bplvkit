import json
import os
import sqlite3
from datetime import datetime, time
from typing import Dict, List, Optional
from decimal import Decimal

class TransactionTools:
    def calculate_tax(self, amount: Decimal, tax_category: str) -> Decimal:
        """Calculate tax for a given amount and category"""
        tax_rates = {
            'pour/shot': Decimal('0.07'),
            'glass': Decimal('0.07'),
            'bottle': Decimal('0.09'),
            'event': Decimal('0.10')
        }
        return amount * tax_rates.get(tax_category, Decimal('0.07'))

    def format_currency(self, amount: Decimal) -> str:
        """Format amount as currency"""
        return f"${amount:.2f}"

    def validate_payment_method(self, method: str) -> bool:
        """Validate payment method"""
        valid_methods = ['cash', 'credit', 'debit', 'mobile']
        return method.lower() in valid_methods

class EventTools:
    def validate_event_type(self, event_type: str) -> bool:
        """Validate event type"""
        valid_types = ['wedding', 'corporate', 'private']
        return event_type.lower() in valid_types

    def validate_date_time(self, date: str, time: str) -> bool:
        """Validate date and time format"""
        try:
            datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M")
            return True
        except ValueError:
            return False

    def get_drink_packages(self) -> Dict[str, Dict]:
        """Get available drink packages"""
        return {
            'basic': {
                'name': 'Basic Package',
                'price': Decimal('1500.00'),
                'description': 'House beer, wine, and spirits'
            },
            'premium': {
                'name': 'Premium Package',
                'price': Decimal('2500.00'),
                'description': 'Premium beer, wine, and top-shelf spirits'
            },
            'luxury': {
                'name': 'Luxury Package',
                'price': Decimal('3500.00'),
                'description': 'All premium options plus champagne service'
            }
        }

class RevenueTools:
    def calculate_shift_revenue(self, transactions: List[Dict]) -> Dict:
        """Calculate revenue for a shift"""
        total_sales = sum(t['total_amount'] for t in transactions)
        total_tax = sum(t['tax_amount'] for t in transactions)
        return {
            'total_sales': total_sales,
            'total_tax': total_tax,
            'transaction_count': len(transactions),
            'average_transaction': total_sales / len(transactions) if transactions else 0
        }

    def format_summary(self, summary: Dict) -> str:
        """Format revenue summary for display"""
        return f"""Revenue Summary:
Sales: ${summary['total_sales']:.2f}
Tax: ${summary['total_tax']:.2f}
Transactions: {summary['transaction_count']}
Average Transaction: ${summary['average_transaction']:.2f}"""

class BevTools:
    def __init__(self):
        self.transaction_tools = TransactionTools()
        self.event_tools = EventTools()
        self.revenue_tools = RevenueTools()
        self.order_history: List[Dict] = []
        self.inventory: Dict[str, int] = {}
        self.menu: Dict[str, Dict] = {}
        self.active_orders: Dict[str, Dict] = {}
        
        # Initialize database if it doesn't exist
        self.db_file = "orders.db"
        self.initialize_db()
        
        # Load drinks data from JSON
        with open('drinks.json', 'r') as f:
            self.drinks_data = json.load(f)

    def initialize_db(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        # Create orders table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            timestamp TEXT,
            items TEXT,
            total REAL,
            payment_method TEXT
        )
        ''')
        
        conn.commit()
        conn.close()

    # Create operations
    def create_drink(self, name: str, category: str, subcategory: str, price: float, description: str = "") -> bool:
        if name in self.menu:
            return False
        self.menu[name] = {
            "category": category,
            "subcategory": subcategory,
            "price": price,
            "description": description,
            "created_at": datetime.now().isoformat()
        }
        self.inventory[name] = 0
        return True

    def create_order(self, customer_name: str) -> str:
        order_id = f"ORD{len(self.active_orders) + 1}"
        self.active_orders[order_id] = {
            "customer": customer_name,
            "items": [],
            "status": "pending",
            "created_at": datetime.now().isoformat()
        }
        return order_id

    # Read operations
    def get_drink(self, name: str) -> Optional[Dict]:
        return self.menu.get(name)

    def get_order(self, order_id: str) -> Optional[Dict]:
        return self.active_orders.get(order_id)

    def list_drinks_by_category(self, category: str) -> List[Dict]:
        return {name: details for name, details in self.menu.items() 
                if details["category"] == category}

    def search_drinks(self, query: str) -> List[str]:
        return [name for name in self.menu.keys() 
                if query.lower() in name.lower()]

    # Update operations
    def update_drink(self, name: str, **updates) -> bool:
        if name not in self.menu:
            return False
        self.menu[name].update(updates)
        return True

    def update_order_status(self, order_id: str, status: str) -> bool:
        if order_id not in self.active_orders:
            return False
        self.active_orders[order_id]["status"] = status
        if status == "completed":
            self.order_history.append(self.active_orders[order_id])
        return True

    def add_to_order(self, order_id: str, drink_name: str, quantity: int = 1) -> bool:
        if order_id not in self.active_orders or drink_name not in self.menu:
            return False
        if self.inventory.get(drink_name, 0) < quantity:
            return False
        
        self.active_orders[order_id]["items"].append({
            "drink": drink_name,
            "quantity": quantity,
            "price": self.menu[drink_name]["price"] * quantity
        })
        self.inventory[drink_name] -= quantity
        return True

    # Delete operations
    def delete_drink(self, name: str) -> bool:
        if name not in self.menu:
            return False
        del self.menu[name]
        del self.inventory[name]
        return True

    def cancel_order(self, order_id: str) -> bool:
        if order_id not in self.active_orders:
            return False
        # Return items to inventory
        for item in self.active_orders[order_id]["items"]:
            self.inventory[item["drink"]] += item["quantity"]
        del self.active_orders[order_id]
        return True

    # Inventory operations
    def update_inventory(self, drink_name: str, quantity: int) -> bool:
        if drink_name not in self.menu:
            return False
        self.inventory[drink_name] = quantity
        return True

    def get_inventory_level(self, drink_name: str) -> Optional[int]:
        return self.inventory.get(drink_name)

    def check_low_inventory(self, drink_name: str, threshold_percent: float = 0.3) -> bool:
        """Check if inventory is below threshold percentage"""
        if drink_name not in self.menu:
            return False
        
        current_level = self.inventory.get(drink_name, 0)
        # Assuming initial inventory was 100 units
        initial_inventory = 100
        return (current_level / initial_inventory) < threshold_percent

    def send_inventory_notification(self, drink_name: str) -> str:
        """Generate notification message for low inventory"""
        current_level = self.inventory.get(drink_name, 0)
        message = f"Low inventory alert for {drink_name}: {current_level} units remaining"
        # In a real system, you would implement actual notification logic here
        return message

    def process_transaction(self, items: List[Dict], payment_method: str) -> Optional[Dict]:
        """Process a complete transaction"""
        if not self.transaction_tools.validate_payment_method(payment_method):
            return None

        total_amount = Decimal('0')
        total_tax = Decimal('0')
        processed_items = []

        for item in items:
            if item['id'] not in self.menu:
                continue
                
            price = Decimal(str(self.menu[item['id']]['price']))
            quantity = item['quantity']
            line_total = price * quantity
            tax = self.transaction_tools.calculate_tax(line_total, item.get('tax_category', 'pour/shot'))
            
            processed_items.append({
                'id': item['id'],
                'quantity': quantity,
                'unit_price': price,
                'line_total': line_total,
                'tax': tax
            })
            
            total_amount += line_total
            total_tax += tax

        return {
            'items': processed_items,
            'total_amount': total_amount,
            'total_tax': total_tax,
            'payment_method': payment_method,
            'timestamp': datetime.now().isoformat()
        }

    def create_event_booking(self, 
                           event_details: Dict,
                           package_type: str) -> Optional[Dict]:
        """Create an event booking with drink package"""
        if not self.event_tools.validate_event_type(event_details['type']):
            return None
            
        if not self.event_tools.validate_date_time(
            event_details['date'], 
            event_details['time']
        ):
            return None

        packages = self.event_tools.get_drink_packages()
        if package_type not in packages:
            return None

        return {
            'event': event_details,
            'drink_package': packages[package_type],
            'booking_time': datetime.now().isoformat(),
            'status': 'confirmed'
        }

    def generate_revenue_report(self, 
                              date: Optional[str] = None,
                              shift: Optional[str] = None) -> Optional[Dict]:
        """Generate revenue report for date/shift"""
        transactions = [t for t in self.order_history 
                       if self._matches_date_shift(t, date, shift)]
        
        if not transactions:
            return None
            
        return self.revenue_tools.calculate_shift_revenue(transactions)

    def _matches_date_shift(self, 
                          transaction: Dict,
                          date: Optional[str],
                          shift: Optional[str]) -> bool:
        """Check if transaction matches date/shift criteria"""
        if date and date not in transaction['timestamp']:
            return False
        
        if shift:
            trans_time = datetime.fromisoformat(transaction['timestamp']).time()
            if shift == 'morning' and not (8 <= trans_time.hour < 16):
                return False
            if shift == 'evening' and not (16 <= trans_time.hour < 24):
                return False
                
        return True

    def process_order(self, items, payment_method="cash"):
        """Process a drink order and save it to the database"""
        try:
            # Calculate total
            total = 0
            processed_items = []
            
            for item in items:
                drink_name = item.get("name")
                quantity = item.get("quantity", 1)
                
                # Find the drink in our data
                drink = next((d for d in self.drinks_data if d.get("name") == drink_name), None)
                
                if drink:
                    price = drink.get("price", 0) * quantity
                    total += price
                    
                    # Update inventory and sales
                    current_inventory = drink.get("inventory", 0)
                    current_sales = drink.get("sales", 0)
                    
                    drink["inventory"] = current_inventory - quantity
                    drink["sales"] = current_sales + quantity
                    
                    processed_items.append({
                        "name": drink_name,
                        "quantity": quantity,
                        "price": price
                    })
            
            # Save updated inventory back to JSON
            with open('drinks.json', 'w') as f:
                json.dump(self.drinks_data, f, indent=2)
            
            # Save order to database
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Convert datetime to string if needed
            current_time = datetime.now()
            if isinstance(current_time, datetime) or isinstance(current_time, time):
                timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
            else:
                timestamp = str(current_time)
            
            cursor.execute(
                "INSERT INTO orders (timestamp, items, total, payment_method) VALUES (?, ?, ?, ?)",
                (timestamp, json.dumps(processed_items), total, payment_method)
            )
            
            order_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            return {
                "order_id": order_id,
                "items": processed_items,
                "total": total,
                "payment_method": payment_method,
                "timestamp": timestamp
            }
            
        except Exception as e:
            return {"error": str(e)}
    
    def lookup_beverage(self, name=None, category=None, subcategory=None, max_price=None):
        """Search for beverages based on criteria"""
        results = []
        
        for drink in self.drinks_data:
            # Skip incomplete entries
            if not drink.get("name") or not drink.get("category"):
                continue
                
            match = True
            
            if name and name.lower() not in drink.get("name", "").lower():
                match = False
                
            if category and category.lower() != drink.get("category", "").lower():
                match = False
                
            if subcategory and subcategory.lower() != drink.get("subcategory", "").lower():
                match = False
                
            if max_price and drink.get("price", 0) > float(max_price):
                match = False
                
            if match:
                results.append(drink)
        
        return results
