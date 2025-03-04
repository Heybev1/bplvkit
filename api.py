from livekit.agents import llm
import enum
from typing import Annotated, Optional, List, Dict, Any
import logging
from db_driver import DatabaseDriver
import json
from visualization import Visualizer

logger = logging.getLogger("user-data")
logger.setLevel(logging.INFO)

DB = DatabaseDriver()
VIZ = Visualizer(DB)

class BevDetails(enum.Enum):
    ID = "id"
    Name = "name"
    Category = "category"
    Subcategory = "subcategory"
    Price = "price"
    Inventory = "inventory"
    Image = "image"
    Sales = "sales"

class AssistantFnc(llm.FunctionContext):
    def __init__(self):
        super().__init__()
        
        self._bev_details = {
            BevDetails.ID: "",
            BevDetails.Name: "",
            BevDetails.Category: "",
            BevDetails.Subcategory: "",
            BevDetails.Price: 0,
            BevDetails.Inventory: 0,
            BevDetails.Image: "",
            BevDetails.Sales: 0
        }
        
        self._current_batch_id = None
    
    def get_bev_str(self):
        bev_str = ""
        for key, value in self._bev_details.items():
            bev_str += f"{key.value}: {value}\n"
        return bev_str
    
    @llm.ai_callable(description="lookup a beverage by its id or name")
    def lookup_bev(self, bev_id: Annotated[str, llm.TypeInfo(description="The id or name of the beverage to lookup")]):
        logger.info("lookup bev - id/name: %s", bev_id)
        
        # Try exact ID match first
        result = DB.get_bev_by_id(bev_id)
        
        # If not found, try generating ID from name
        if result is None:
            generated_id = DB._generate_id(bev_id)
            result = DB.get_bev_by_id(generated_id)
            
        if result is None:
            return "Beverage not found"
        
        self._bev_details = {
            BevDetails.ID: result.id,
            BevDetails.Name: result.name,
            BevDetails.Category: result.category,
            BevDetails.Subcategory: result.subcategory,
            BevDetails.Price: result.price,
            BevDetails.Inventory: result.inventory,
            BevDetails.Image: result.image,
            BevDetails.Sales: result.sales
        }
        
        return f"The beverage details are:\n{self.get_bev_str()}"
    
    @llm.ai_callable(description="get the details of the current beverage")
    def get_bev_details(self):
        logger.info("get beverage details")
        return f"The beverage details are:\n{self.get_bev_str()}"
    
    @llm.ai_callable(description="create a new beverage")
    def create_bev(
        self, 
        bev_id: Annotated[str, llm.TypeInfo(description="The id of the beverage")],
        name: Annotated[str, llm.TypeInfo(description="The name of the beverage")],
        category: Annotated[str, llm.TypeInfo(description="The category of the beverage")],
        subcategory: Annotated[str, llm.TypeInfo(description="The subcategory of the beverage")],
        price: Annotated[int, llm.TypeInfo(description="The price of the beverage")],
        inventory: Annotated[int, llm.TypeInfo(description="The inventory of the beverage")],
        image: Annotated[str, llm.TypeInfo(description="The image URL of the beverage")],
        sales: Annotated[int, llm.TypeInfo(description="The sales of the beverage")] = 0
    ):
        logger.info("create beverage - id: %s, name: %s, category: %s, subcategory: %s, price: %d, inventory: %d, image: %s, sales: %d", bev_id, name, category, subcategory, price, inventory, image, sales)
        result = DB.create_bev(bev_id, name, category, subcategory, price, inventory, image, sales)
        if result is None:
            return "Failed to create beverage"
        
        self._bev_details = {
            BevDetails.ID: result.id,
            BevDetails.Name: result.name,
            BevDetails.Category: result.category,
            BevDetails.Subcategory: result.subcategory,
            BevDetails.Price: result.price,
            BevDetails.Inventory: result.inventory,
            BevDetails.Image: result.image,
            BevDetails.Sales: result.sales
        }
        
        return "beverage created!"
    
    def has_bev(self):
        return self._bev_details[BevDetails.ID] != ""
    
    @llm.ai_callable(description="check inventory levels and notify if low")
    def check_inventory_levels(self):
        if not self.has_bev():
            return "No beverage selected"
        
        bev_name = self._bev_details[BevDetails.Name]
        inventory = self._bev_details[BevDetails.Inventory]
        initial_inventory = 100  # Assuming initial inventory was 100 units
        
        if (inventory / initial_inventory) < 0.3:
            notify_msg = f"⚠️ Low inventory alert!\n{bev_name} is running low ({inventory} units). Would you like me to notify Brian and Chris to place a reorder?"
            return notify_msg
        return None

    @llm.ai_callable(description="update an existing beverage's details")
    def update_bev(
        self,
        bev_id: Annotated[str, llm.TypeInfo(description="The id of the beverage to update")],
        name: Annotated[str, llm.TypeInfo(description="New name")] = None,
        category: Annotated[str, llm.TypeInfo(description="New category")] = None,
        subcategory: Annotated[str, llm.TypeInfo(description="New subcategory")] = None,
        price: Annotated[int, llm.TypeInfo(description="New price")] = None,
        inventory: Annotated[int, llm.TypeInfo(description="New inventory")] = None,
        image: Annotated[str, llm.TypeInfo(description="New image URL")] = None
    ):
        """Update beverage details"""
        current_bev = DB.get_bev_by_id(bev_id)
        if not current_bev:
            return "Beverage not found"
            
        # Use existing values if new ones aren't provided
        name = name or current_bev.name
        category = category or current_bev.category
        subcategory = subcategory or current_bev.subcategory
        price = price if price is not None else current_bev.price
        inventory = inventory if inventory is not None else current_bev.inventory
        image = image or current_bev.image
        
        result = DB.create_bev(bev_id, name, category, subcategory, price, inventory, image, current_bev.sales)
        if result:
            self._bev_details.update({
                BevDetails.ID: result.id,
                BevDetails.Name: result.name,
                BevDetails.Category: result.category,
                BevDetails.Subcategory: result.subcategory,
                BevDetails.Price: result.price,
                BevDetails.Inventory: result.inventory,
                BevDetails.Image: result.image,
                BevDetails.Sales: result.sales
            })
            return "Beverage updated successfully!"
        return "Failed to update beverage"

    @llm.ai_callable(description="delete a beverage from the database")
    def delete_bev(self, bev_id: Annotated[str, llm.TypeInfo(description="The id of the beverage to delete")]):
        """Delete a beverage"""
        if DB.delete_bev(bev_id):
            if self._bev_details[BevDetails.ID] == bev_id:
                self._bev_details = {key: "" if isinstance(value, str) else 0 for key, value in self._bev_details.items()}
            return "Beverage deleted successfully!"
        return "Failed to delete beverage"

    @llm.ai_callable(description="list all beverages in a category")
    def list_bevs_by_category(self, category: Annotated[str, llm.TypeInfo(description="The category to list beverages from")]):
        """List all beverages in a category"""
        bevs = DB.get_bevs_by_category(category)
        if not bevs:
            return f"No beverages found in category: {category}"
        
        result = f"Beverages in {category}:\n"
        for bev in bevs:
            result += f"- {bev.name} (${bev.price/100:.2f})\n"
        return result

    @llm.ai_callable(description="create a new transaction")
    def create_transaction(
        self,
        items_json: Annotated[str, llm.TypeInfo(description="JSON string of items array with id and quantity")],
        payment_method: Annotated[str, llm.TypeInfo(description="Payment method used")],
        employee_id: Annotated[Optional[int], llm.TypeInfo(description="ID of employee processing transaction")] = None
    ):
        """Create a new transaction with items"""
        try:
            items = json.loads(items_json)  # Convert JSON string to list
            
            # Ensure items are properly formatted
            formatted_items = []
            for item in items:
                if isinstance(item, dict) and "id" in item and "quantity" in item:
                    # Ensure quantity is an integer
                    item["quantity"] = int(item["quantity"])
                    formatted_items.append(item)
            
            if not formatted_items:
                return "No valid items found in the transaction"
                
            transaction_id = DB.create_transaction(payment_method, formatted_items, employee_id)
            if transaction_id:
                return f"Transaction {transaction_id} created successfully!"
            return "Failed to create transaction"
        except json.JSONDecodeError:
            return "Invalid items format. Please provide a valid JSON array of items."
        except Exception as e:
            return f"Error processing transaction: {str(e)}"

    @llm.ai_callable(description="create a new event booking")
    def create_event(
        self,
        event_name: Annotated[str, llm.TypeInfo(description="Name of the event")],
        event_type: Annotated[str, llm.TypeInfo(description="Type of event (wedding/corporate)")],
        event_date: Annotated[str, llm.TypeInfo(description="Date of the event (YYYY-MM-DD)")],
        event_time: Annotated[str, llm.TypeInfo(description="Time of the event (HH:MM)")],
        venue: Annotated[str, llm.TypeInfo(description="Venue location")],
        client_id: Annotated[int, llm.TypeInfo(description="ID of the client")] = None,
        description: Annotated[str, llm.TypeInfo(description="Event description")] = None
    ):
        """Create a new event booking"""
        event_id = DB.create_event(event_name, event_type, event_date, event_time, venue, client_id, description)
        if event_id:
            return f"Event {event_id} created successfully!"
        return "Failed to create event"

    @llm.ai_callable(description="add a drink package to an event")
    def add_event_package(
        self,
        event_id: Annotated[int, llm.TypeInfo(description="ID of the event")],
        package_type: Annotated[str, llm.TypeInfo(description="Type of drink package")],
        cost: Annotated[float, llm.TypeInfo(description="Cost of the package")],
        details: Annotated[str, llm.TypeInfo(description="Package details")] = None
    ):
        """Add a drink package to an event"""
        booking_id = DB.create_event_booking(event_id, "drink_package", package_type, cost, details)
        if booking_id:
            return f"Drink package added to event {event_id}!"
        return "Failed to add drink package"

    @llm.ai_callable(description="get event details")
    def get_event_details(
        self,
        event_id: Annotated[int, llm.TypeInfo(description="ID of the event to look up")]
    ):
        """Get details of an event"""
        event = DB.get_event_details(event_id)
        if not event:
            return "Event not found"
        return f"Event Details:\n{event}"

    @llm.ai_callable(description="get revenue summary")
    def get_revenue_summary(
        self,
        date: Annotated[str, llm.TypeInfo(description="Date to get summary for (YYYY-MM-DD)")] = None,
        shift: Annotated[str, llm.TypeInfo(description="Specific shift to get summary for")] = None
    ):
        """Get revenue summary for a date/shift"""
        summary = DB.get_revenue_summary(date, shift)
        if not summary:
            return "No revenue data found"
        return f"Revenue Summary:\n{summary}"

    @llm.ai_callable(description="generate a visual representation of the menu")
    def visualize_menu(self, category: Annotated[Optional[str], llm.TypeInfo(description="Category to visualize")] = None):
        """Generate a visual menu representation"""
        try:
            chart_data = VIZ.generate_visual_menu(category)
            return {
                "chart": chart_data,
                "message": f"Visual menu for {'all categories' if not category else category}"
            }
        except Exception as e:
            return f"Error generating visual menu: {str(e)}"

    @llm.ai_callable(description="generate a sales trend chart")
    def visualize_sales_trend(self, days: Annotated[int, llm.TypeInfo(description="Number of days to include")] = 30):
        """Generate a sales trend chart"""
        try:
            chart_data = VIZ.generate_sales_trend(days)
            return {
                "chart": chart_data,
                "message": f"Sales trend for the last {days} days"
            }
        except Exception as e:
            return f"Error generating sales trend: {str(e)}"

    @llm.ai_callable(description="generate a receipt for a transaction")
    def generate_receipt(self, transaction_id: Annotated[int, llm.TypeInfo(description="Transaction ID to generate receipt for")]):
        """Generate a receipt for a transaction"""
        try:
            receipt = DB.generate_receipt(transaction_id)
            if "error" in receipt:
                return receipt["error"]
            
            # Format receipt as text
            receipt_text = f"Receipt #{receipt['transaction_id']}\n"
            receipt_text += f"Date: {receipt['date']} {receipt['time']}\n\n"
            receipt_text += "Items:\n"
            
            for item in receipt['items']:
                receipt_text += f"  {item['name']} x{item['quantity']} @ ${item['unit_price']/100:.2f} = ${item['total']/100:.2f}\n"
            
            receipt_text += f"\nSubtotal: ${receipt['subtotal']/100:.2f}\n"
            receipt_text += f"Tax: ${receipt['tax']/100:.2f}\n"
            receipt_text += f"Total: ${receipt['total']/100:.2f}\n"
            receipt_text += f"Payment method: {receipt['payment_method']}\n"
            
            return receipt_text
        except Exception as e:
            return f"Error generating receipt: {str(e)}"

    @llm.ai_callable(description="get beverage recommendations")
    def get_recommendations(
        self,
        bev_id: Annotated[Optional[str], llm.TypeInfo(description="Beverage ID to get recommendations for")],
        limit: Annotated[int, llm.TypeInfo(description="Maximum number of recommendations")] = 3
    ):
        """Get recommendations for a beverage"""
        try:
            # If no bev_id provided, use current selected beverage
            if not bev_id and self.has_bev():
                bev_id = self._bev_details[BevDetails.ID]
            
            if not bev_id:
                return "No beverage selected for recommendations"
            
            recommendations = DB.get_recommendations(bev_id, limit)
            
            if not recommendations:
                return f"No recommendations found for {bev_id}"
            
            # Format recommendations as text
            result = f"Recommendations based on {bev_id}:\n"
            for rec in recommendations:
                result += f"- {rec['name']} (${rec['price']/100:.2f}) - {rec['confidence']*100:.0f}% match\n"
            
            return result
        except Exception as e:
            return f"Error getting recommendations: {str(e)}"

    @llm.ai_callable(description="create a batch order")
    def create_batch_order(
        self,
        table_number: Annotated[str, llm.TypeInfo(description="Table number or identifier")],
        customer_id: Annotated[Optional[int], llm.TypeInfo(description="Customer ID if available")] = None
    ):
        """Create a new batch order"""
        try:
            batch_id = DB.create_batch_order(table_number, customer_id)
            if batch_id:
                self._current_batch_id = batch_id
                return f"Batch order #{batch_id} created for table {table_number}"
            return "Failed to create batch order"
        except Exception as e:
            return f"Error creating batch order: {str(e)}"

    @llm.ai_callable(description="add an item to the current batch order")
    def add_to_batch(
        self,
        bev_id: Annotated[str, llm.TypeInfo(description="Beverage ID to add")],
        quantity: Annotated[int, llm.TypeInfo(description="Quantity to add")] = 1,
        notes: Annotated[Optional[str], llm.TypeInfo(description="Special instructions")] = None
    ):
        """Add an item to the current batch order"""
        try:
            if not self._current_batch_id:
                return "No active batch order. Create one first."
            
            # Try exact ID match first
            result = DB.get_bev_by_id(bev_id)
            
            # If not found, try generating ID from name
            if result is None:
                generated_id = DB._generate_id(bev_id)
                result = DB.get_bev_by_id(generated_id)
                if result:
                    bev_id = generated_id
            
            if result is None:
                return f"Beverage '{bev_id}' not found"
            
            if DB.add_to_batch(self._current_batch_id, bev_id, quantity, notes):
                return f"Added {quantity}x {result.name} to batch #{self._current_batch_id}"
            return "Failed to add item to batch"
        except Exception as e:
            return f"Error adding to batch: {str(e)}"

    @llm.ai_callable(description="get the current batch order details")
    def get_batch_order(self):
        """Get the current batch order details"""
        try:
            if not self._current_batch_id:
                return "No active batch order"
            
            batch = DB.get_batch_order(self._current_batch_id)
            if "error" in batch:
                return batch["error"]
            
            # Format batch as text
            batch_text = f"Batch Order #{batch['batch_id']}\n"
            batch_text += f"Table: {batch['table_number']}\n"
            batch_text += f"Status: {batch['status']}\n\n"
            batch_text += "Items:\n"
            
            for item in batch['items']:
                batch_text += f"  {item['name']} x{item['quantity']} @ ${item['unit_price']/100:.2f}"
                if item['notes']:
                    batch_text += f" - Note: {item['notes']}"
                batch_text += "\n"
            
            batch_text += f"\nSubtotal: ${batch['subtotal']/100:.2f}"
            return batch_text
        except Exception as e:
            return f"Error getting batch order: {str(e)}"

    @llm.ai_callable(description="finalize the current batch order")
    def finalize_batch(
        self,
        payment_method: Annotated[str, llm.TypeInfo(description="Payment method to use")],
        employee_id: Annotated[Optional[int], llm.TypeInfo(description="Employee ID processing the order")] = None
    ):
        """Finalize and process the current batch order"""
        try:
            if not self._current_batch_id:
                return "No active batch order"
            
            transaction_id = DB.finalize_batch(self._current_batch_id, payment_method, employee_id)
            if transaction_id:
                receipt = self.generate_receipt(transaction_id)
                self._current_batch_id = None  # Clear the current batch
                return f"Batch order finalized!\n\n{receipt}"
            return "Failed to finalize batch order"
        except Exception as e:
            return f"Error finalizing batch: {str(e)}"

    @llm.ai_callable(description="cancel the current batch order")
    def cancel_batch(self):
        """Cancel the current batch order"""
        try:
            if not self._current_batch_id:
                return "No active batch order"
            
            if DB.cancel_batch(self._current_batch_id):
                self._current_batch_id = None
                return "Batch order cancelled successfully"
            return "Failed to cancel batch order"
        except Exception as e:
            return f"Error cancelling batch: {str(e)}"