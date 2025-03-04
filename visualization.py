import matplotlib.pyplot as plt
import io
import base64
from typing import List, Dict, Any, Optional
import numpy as np
from db_driver import DatabaseDriver
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

class Visualizer:
    def __init__(self, db: DatabaseDriver):
        self.db = db
    
    def generate_pie_chart(self, data: Dict[str, float], title: str = "Category Distribution") -> str:
        """Generate a pie chart from provided data and return as base64 encoded string"""
        plt.figure(figsize=(10, 6))
        plt.pie(data.values(), labels=data.keys(), autopct='%1.1f%%', startangle=90)
        plt.axis('equal')
        plt.title(title)
        
        # Save plot to a bytes buffer
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        image_png = buffer.getvalue()
        buffer.close()
        plt.close()
        
        # Encode the bytes as base64
        encoded_image = base64.b64encode(image_png).decode('utf-8')
        return f"data:image/png;base64,{encoded_image}"
    
    def generate_bar_chart(self, categories: List[str], values: List[float], 
                           title: str = "Sales by Category", x_label: str = "Categories", 
                           y_label: str = "Sales") -> str:
        """Generate a bar chart and return as base64 encoded string"""
        plt.figure(figsize=(12, 6))
        plt.bar(categories, values)
        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Save plot to a bytes buffer
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        image_png = buffer.getvalue()
        buffer.close()
        plt.close()
        
        # Encode the bytes as base64
        encoded_image = base64.b64encode(image_png).decode('utf-8')
        return f"data:image/png;base64,{encoded_image}"
    
    def generate_visual_menu(self, category: Optional[str] = None) -> Dict[str, Any]:
        """Generate an interactive menu visualization"""
        # Get beverages from database
        if category:
            bevs = self.db.get_bevs_by_category(category)
        else:
            # Get all categories and combine
            bevs = []
            for cat in ["Signature", "Classics", "Beer", "Wine", "Spirits", "Non-Alcoholic"]:
                bevs.extend(self.db.get_bevs_by_category(cat))

        # Convert to DataFrame
        df = pd.DataFrame([{
            'name': bev.name,
            'category': bev.category,
            'subcategory': bev.subcategory,
            'price': bev.price/100,  # Convert cents to dollars
            'inventory': bev.inventory
        } for bev in bevs])

        # Create treemap
        fig = px.treemap(df, 
            path=[px.Constant("Menu"), 'category', 'subcategory', 'name'],
            values='price',
            color='price',
            hover_data=['inventory'],
            color_continuous_scale='RdBu'
        )

        fig.update_layout(
            title="Interactive Menu Visualization",
            width=800,
            height=600
        )

        return fig.to_json()

    def generate_sales_trend(self, days: int = 30) -> Dict[str, Any]:
        """Generate sales trend visualization"""
        trend_data = self.db.get_sales_trend(days)
        
        df = pd.DataFrame(trend_data)
        df['date'] = pd.to_datetime(df['date'])
        
        # Create line chart
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['sales_total'],
            name='Sales',
            line=dict(color='blue')
        ))

        fig.update_layout(
            title=f"Sales Trend - Last {days} Days",
            xaxis_title="Date",
            yaxis_title="Sales Amount ($)",
            width=800,
            height=400,
            showlegend=True
        )

        return fig.to_json()
