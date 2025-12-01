from waitress import serve
from myshop.wsgi import application  # Replace "myproject" with your Django project folder name

serve(application, host='0.0.0.0', port=8001)
