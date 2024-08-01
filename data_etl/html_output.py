import webbrowser

def create_html_from_python(python_file, html_file):
    # Read the content of the Python file
    with open(python_file, 'r') as file:
        python_code = file.read()
    
    # Create the HTML content
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Python Code Display</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
            }}
            pre {{
                background-color: #f4f4f4;
                border: 1px solid #ddd;
                padding: 10px;
                overflow: auto;
            }}
            code {{
                color: #d63384;
            }}
        </style>
    </head>
    <body>
        <h1>Python Script</h1>
        <pre><code>{python_code}</code></pre>
    </body>
    </html>
    """
    
    # Write the HTML content to the HTML file
    with open(html_file, 'w') as file:
        file.write(html_content)
    
    print(f"HTML file '{html_file}' has been created.")


create_html_from_python('ETL.py', 'ETL.html')
