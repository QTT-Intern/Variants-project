from flask import Flask, render_template
import pandas as pd
df_grouped = pd.read_csv('grouped_data_2025.csv')
app = Flask(__name__)

@app.route('/')
def product_listing():
    # Assuming df_grouped is already created and available
    products = df_grouped.to_dict(orient='records')
    return render_template('product_listing.html', products=products)

if __name__ == '__main__':
    app.run(debug=True)