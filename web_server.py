from flask import Flask, render_template_string
from db_driver import DatabaseDriver
from visualization import Visualizer
import json

app = Flask(__name__)
db = DatabaseDriver()
viz = Visualizer(db)

# HTML template with plotly
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Beverage Visualizations</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        .chart-container { margin: 20px; padding: 20px; border: 1px solid #ddd; }
    </style>
</head>
<body>
    <div class="chart-container">
        <h2>Menu Visualization</h2>
        <div id="menu-chart"></div>
    </div>
    <div class="chart-container">
        <h2>Sales Trend</h2>
        <div id="sales-chart"></div>
    </div>

    <script>
        // Parse and render menu chart
        const menuData = JSON.parse({{menu_data|tojson}});
        Plotly.newPlot('menu-chart', menuData.data, menuData.layout);

        // Parse and render sales chart
        const salesData = JSON.parse({{sales_data|tojson}});
        Plotly.newPlot('sales-chart', salesData.data, salesData.layout);
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    menu_data = viz.generate_visual_menu()
    sales_data = viz.generate_sales_trend()
    
    return render_template_string(HTML_TEMPLATE, 
                                menu_data=menu_data,
                                sales_data=sales_data)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
